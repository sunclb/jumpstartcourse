import webvtt
import re
import datetime
import os
import ffmpeg
import logging
import csv
from absl import app
from absl import flags
import math
import wave
from pydub.silence import detect_nonsilent
from pydub import AudioSegment
from random import randint
from pathos.multiprocessing import ProcessingPool as Pool
from multiprocess import Manager
from multiprocessing import  Value, cpu_count
from ctypes import c_int
logging.getLogger().setLevel(logging.INFO)

FLAGS = flags.FLAGS
flags.DEFINE_string("input_folder",None,
	"The folder should contain sub folder webm containing audio files and vtt folder containing subtitle files")
flags.DEFINE_string("output_folder","./cut_output","The dir of output files")
flags.DEFINE_integer("cut_period",1,"cut lower limit in seconds")
flags.DEFINE_boolean("cut_silence",False,"Whether need to cut out silence part")

cut_silence_file_num=Value(c_int,0)
cut_silence_out_file_num=Value(c_int,0)
cut_silence_fail_file=Value(c_int,0)
file_num=Value(c_int,0)
out_file_num=Value(c_int,0)
fail_file=Value(c_int,0)
#process_num=int(cpu_count()-2)
process_num=int(cpu_count())
def mp3gen(folder):
	for root, dirs, files in os.walk(folder):
		for filename in files:
			logging.debug("process file:{}".format(filename))
			yield os.path.join(root, filename)


def unix_time_millis(dt):
	epoch_str='00:00:00.000'
	epoch = datetime.datetime.strptime(epoch_str,'%H:%M:%S.%f')
	return (dt - epoch).total_seconds()


def cut_wave(wave_file,cut_start,cut_end,start_bias=0,end_bias=0):
	newAudio_prop={}
	with wave.open(wave_file, mode='rb') as newAudio:
		newAudio_prop["nchannels"]=newAudio.getnchannels()
		newAudio_prop["nframes"]=newAudio.getnframes()
		newAudio_prop["sampwidth"]=newAudio.getsampwidth()
		newAudio_prop["framerate"]=newAudio.getframerate()
		newAudio_prop["comptype"]=newAudio.getcomptype()
		newAudio_prop["compname"]=newAudio.getcompname()
		cut_duration=cut_end+end_bias-cut_start-start_bias
		cut_nframe=int(math.floor(cut_duration*newAudio_prop["framerate"]))
		#start_bias=datetime.timedelta(seconds=start_bias)
		newAudio.setpos(int(math.floor((cut_start+start_bias)*newAudio_prop["framerate"])))
		cut_audio=newAudio.readframes(cut_nframe)
		newAudio_prop["totalduration"]=newAudio_prop["nframes"]/newAudio_prop["framerate"]
		newAudio_prop["nframes"]=int(len(cut_audio)/newAudio_prop["nchannels"]/newAudio_prop["sampwidth"])
	return cut_audio,newAudio_prop

#cutting criteria: drop al segments below 5s
#assumption: all speech can be cut into defined interval. e.g. (5-10s)
def TimestampMillisec64():
	return int((datetime.datetime.utcnow() - datetime.datetime(1970, 1, 1)).total_seconds() * 1000)
def cut_by_silence(precut_audio_path,output_folder,filebasename):
	if not os.path.exists(output_folder):
		os.mkdir(output_folder)
	#use pydub AudioSegment &silence module to detect silence, cut and save, 
	#return last chunck's time stamp in millionsecond
	cut_num=0
	audio_segment=AudioSegment.from_wav(precut_audio_path)
	silence_thresh_tries=range(-40,-5)
	for silence_thresh in silence_thresh_tries: 
		chuncks = detect_nonsilent(audio_segment,min_silence_len=500,silence_thresh=silence_thresh)
		logging.debug("try {}".format(silence_thresh))
		if len(chuncks)>=2:
			for chunck in chuncks:
				out_audio_file=os.path.join(output_folder,filebasename+"_"+str(TimestampMillisec64())+"_"+str(cut_num)+".wav")
				audio_segment[chunck[0]:chunck[1]].export(out_audio_file,format='wav')
				cut_num=cut_num+1
			break
	if silence_thresh==-5 and len(chuncks)<2:
		out_audio_file=os.path.join(output_folder,filebasename+"_"+str(TimestampMillisec64())+"_"+str(cut_num)+".wav")
		audio_segment[chuncks[0][0]:chuncks[0][1]].export(out_audio_file,format='wav')
	return 60,cut_num

def cut_wav_without_silence(audio_file_path,output_folder,cut_period):
	filebasename=os.path.basename(audio_file_path)
	filebasename,_=os.path.splitext(filebasename)
	temp_file=filebasename+"_temp.wav"
	#get audio properties
	audio_prop={}
	with wave.open(audio_file_path, mode='rb') as newAudio:
		audio_prop["nchannels"]=newAudio.getnchannels()
		audio_prop["nframes"]=newAudio.getnframes()
		audio_prop["sampwidth"]=newAudio.getsampwidth()
		audio_prop["framerate"]=newAudio.getframerate()
		audio_prop["comptype"]=newAudio.getcomptype()
		audio_prop["compname"]=newAudio.getcompname()
	audio_duration=audio_prop["nframes"]/audio_prop["framerate"]

	precut_duration=cut_period*60
	cut_start=0
	cut_return=0
	cut_num=0
	while cut_start<audio_duration:
		cut_end=cut_start+precut_duration
		cut_audio,cutaudio_prop=cut_wave(audio_file_path,cut_start,cut_end,start_bias=0,end_bias=0) 
		with wave.open(temp_file, "wb") as newAudio:
			newAudio.setparams((cutaudio_prop["nchannels"],cutaudio_prop["sampwidth"],
								cutaudio_prop["framerate"],cutaudio_prop["nframes"],
								cutaudio_prop["comptype"],cutaudio_prop["compname"]))
			newAudio.writeframes(cut_audio)
		cut_return,cut=cut_by_silence(temp_file,output_folder,filebasename)
		cut_start=cut_start+precut_duration
		cut_num+=cut
	os.remove(temp_file)
	return cut_num
def folderbase_cut_silence(input_folder,cut_interval):


	output_no_silence=os.path.join(input_folder,"remove_silence")
	# if not os.path.exists(output_folder):
	# 	os.mkdir(output_folder)
	if not os.path.exists(output_no_silence):
		os.mkdir(output_no_silence)
	wav_files=[]
	for root,dirs,files in os.walk(input_folder):
		for filename in files:
			wav_files.append(filename)
	def process_files(lock,file):
		try:
			#exclude log.txt file
			if re.search(".+\.wav",file):
				wave_file=os.path.join(input_folder,file)
				wo_num=cut_wav_without_silence(wave_file,output_no_silence,cut_interval)
				with cut_silence_file_num.get_lock():
					cut_silence_file_num.value+=1
				with cut_silence_out_file_num.get_lock():
					cut_silence_out_file_num.value+=wo_num
				os.remove(wave_file)

		except Exception as e:
			logging.info(e)
			with cut_silence_fail_file.get_lock():
				cut_silence_fail_file.value+=1
	pool = Pool(process_num)
	m=Manager()
	lock=m.Lock()
	locks=[lock]*len(wav_files)
	pool.map(process_files, locks,wav_files)
	loginfo='''Total number of audio files processed is {}, generated {} files and {} files failed
		'''.format(
			cut_silence_file_num.value,cut_silence_out_file_num.value,cut_silence_fail_file.value)
	logging.info(loginfo)

def folderbase_cut_interval(input_folder,output_folder,cut_period):
	wav_files=[]
	if not os.path.exists(output_folder):
		os.mkdir(output_folder)
	for root,dirs,files in os.walk(input_folder):
		for filename in files:
			wav_files.append(os.path.join(root,filename))
#	for file in wav_files:
	def process_files(lock,file):
		try:
			if re.search(".+\.wav",file):
				with file_num.get_lock():
					file_num.value+=1
				filebasename=os.path.basename(file)
				filebasename,_=os.path.splitext(filebasename)
				#get audio properties
				audio_prop={}
				with wave.open(file, mode='rb') as newAudio:
					audio_prop["nchannels"]=newAudio.getnchannels()
					audio_prop["nframes"]=newAudio.getnframes()
					audio_prop["sampwidth"]=newAudio.getsampwidth()
					audio_prop["framerate"]=newAudio.getframerate()
					audio_prop["comptype"]=newAudio.getcomptype()
					audio_prop["compname"]=newAudio.getcompname()
				audio_duration=audio_prop["nframes"]/audio_prop["framerate"]

				precut_duration=cut_period
				cut_start=0
				cut_return=0
				cut_num=0
				index=0
				while cut_start<audio_duration:
					cut_end=cut_start+precut_duration
					cut_audio,cutaudio_prop=cut_wave(file,cut_start,cut_end,start_bias=0,end_bias=0)
					newfile=os.path.join(output_folder,filebasename+"_"+str(index)+".wav")
					index+=1
					with wave.open(newfile, "wb") as newAudio:
						newAudio.setparams((cutaudio_prop["nchannels"],cutaudio_prop["sampwidth"],
											cutaudio_prop["framerate"],cutaudio_prop["nframes"],
											cutaudio_prop["comptype"],cutaudio_prop["compname"]))
						newAudio.writeframes(cut_audio)
					cut_start=cut_start+precut_duration
					with out_file_num.get_lock():
						out_file_num.value+=1
				os.remove(file)
		except Exception as e:
			logging.info(e)
			with fail_file.get_lock():
				fail_file.value+=1
	pool = Pool(process_num)
	m=Manager()
	lock=m.Lock()
	locks=[lock]*len(wav_files)
	pool.map(process_files, locks,wav_files)
	loginfo='''Total number of audio files processed is {}, generated {} files and {} files failed
	'''.format(file_num.value,out_file_num.value,fail_file.value)
	logging.info(loginfo)	


	
def folderbased_cut_speech(input_folder,output_folder,cut_period,cut_silence):
	if cut_silence:
		wavefolder_without_silence=os.path.join(input_folder,"remove_silence")
		folderbase_cut_silence(input_folder,cut_period)
		folderbase_cut_interval(wavefolder_without_silence,output_folder,cut_period)
	else: 
		folderbase_cut_interval(input_folder,output_folder,cut_period)
	
def main(__):
	#load input_file and parse file_name, link
	logging.getLogger().setLevel(logging.INFO)

	input_folder=FLAGS.input_folder
	output_folder=FLAGS.output_folder
	cut_period=FLAGS.cut_period
	cut_silence=FLAGS.cut_silence
	folderbased_cut_speech(input_folder,output_folder,cut_period,cut_silence)

	
if __name__ == "__main__":

	app.run(main)
