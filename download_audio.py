from __future__ import unicode_literals
import youtube_dl
import os
import re
import csv
import ffmpeg
import datetime
from absl import app
from absl import flags
import logging
'''
#download youtube audio as original format from link provided or file containing links 
#output_folder: contains all downloaded audio files in webm folder, subtitle files in vtt folder, log files: column 1: file base name, 2. webm file path, 3. subtitle file path
#examples in commend line:
# file containing links
EXPORT input_file="bahasa_data_file.txt"
EXPORT output_folder="./output"
EXPORT subtitle_language="id"
python download_audio.py \
    --input_file=$input_file \
    --output_folder=$output_folder \
    --subtitle_language=$subtitle_language
# single link 
EXPORT link='https://www.youtube.com/watch?v=dbybH_h4lCs'
EXPORT filename="list1"
EXPORT output_folder="./output"
EXPORT subtitle_language="en"
!python download_audio.py \
    --link=$link \
    --filename=$filename \
    --output_folder=$output_folder \
    --subtitle_language=$subtitle_language
'''

FLAGS = flags.FLAGS
flags.DEFINE_string("input_file",None,
	"The file contains links to be downloaded. it should be in the format of:"
	"row 1: header; column 2: link, column 3: clean/noise")
flags.DEFINE_string("link",None,"the link to be downloaded,required if input_file is not provided")
flags.DEFINE_string("filename","audio","needed if link is provided, default value is audio")
flags.DEFINE_string("output_folder","./output","The dir of output files")
flags.DEFINE_string("subtitle_language","en","the subtitle language, engilish is default, id is indonesia")


class MyLogger(object):
	def debug(self, msg):
		pass

	def warning(self, msg):
		pass

	def error(self, msg):
		print(msg)

def move_file(current_folder,filename,output_folder,subtitle_language,link):
		audio_folder=os.path.join(output_folder,"webm")
		fileext=os.path.splitext(filename)[1]
		webm_path=os.path.join(current_folder,filename)
		vttext="."+".".join([subtitle_language,'vtt'])
		subtitle_path=re.sub(fileext,vttext,webm_path)
		if not os.path.exists(output_folder):
			os.mkdir(output_folder)
		if not os.path.exists(audio_folder):
			os.mkdir(audio_folder)
		des_webm_path=os.path.join(audio_folder,filename)
		subtitle_folder=os.path.join(output_folder,"vtt")
		if not os.path.exists(subtitle_folder):
			os.mkdir(subtitle_folder)		
		des_subtitle_path=re.sub(current_folder,subtitle_folder,subtitle_path)
		os.rename(webm_path,des_webm_path)
		if os.path.isfile(subtitle_path):
			os.rename(subtitle_path,des_subtitle_path)
		else: des_subtitle_path=None
		#write log file
		log_path=os.path.join(output_folder,"log.txt")
		with open(log_path,"a") as logFile:
			writer=csv.writer(logFile,delimiter="\t")
			line=[datetime.datetime.now(),filename,des_webm_path,des_subtitle_path,link]
			writer.writerow(line)

def download_webm(link,filename,output_folder,subtitle_language):
	def my_hook(d):
		if d['status'] == 'finished':
			print('Done downloading {} in folder {}'.format(filename,output_folder))
	def get_webm_w_subtitle(filename,subtitle_language):
		ydl_opts = {
			'format': 'bestaudio/best',
			'logging': MyLogger(),
			'progress_hooks': [my_hook],
			'outtmpl': '{}_%(id)s.%(ext)s'.format(filename),
			'writesubtitles':True,
			'writeautomaticsub':True,
			'subtitleslangs':subtitle_language,
			'prefer_ffmpeg':True,
			'sleep_interval':1,
			'max_sleep_interval':5}
		return ydl_opts
	ydl_opts=get_webm_w_subtitle(filename,[subtitle_language])
	with youtube_dl.YoutubeDL(ydl_opts) as ydl:
		info=ydl.extract_info(link)
	dir_path=os.getcwd()
	#dir_path = os.path.dirname(os.path.realpath(__file__))
	succ_num=0
	try: 
		entries=info["entries"]
		for entry in entries:
			file_name=filename+"_"+entry["id"]+"."+entry["ext"]
			move_file(dir_path,file_name,output_folder,subtitle_language,link)
			succ_num+=1
			
	except KeyError:	
		file_name=filename+"_"+info["id"]+"."+info["ext"]
		logging.debug("keyerror")
		move_file(dir_path,file_name,output_folder,subtitle_language,link)
		succ_num+=1
	return succ_num

def download_audio(input_file=None,link=None,filename="audio",output_folder="./output",subtitle_language="en"):
	link_num=0
	succ_num=0
	total_file=0
	fail_link=0
	if input_file:
		with open(input_file, 'r') as csvFile:
			reader=csv.reader(csvFile,delimiter="\t")
			for (i,row) in enumerate(reader):
				if i==0: continue
				link=row[1]
				filename=row[3]+"-"+str(i)
				try:
					link_num+=1
					succ=download_webm(link,filename,output_folder,subtitle_language)
					succ_num+=1
					total_file+=succ
					
				except Exception as e:  
					#write log file
					log_path=os.path.join(output_folder,"log.txt")
					with open(log_path,"a") as logFile:
						writer=csv.writer(logFile,delimiter="\t")
						line=[datetime.datetime.now(),filename,"failed",e,link]
						logging.info(e)
						writer.writerow(line)
					fail_link+=1
					continue
	else: 
		if not FLAGS.link:
			print("Either input_file or link is required")
		else:
				try:
					link_num+=1
					succ=download_webm(link,filename,output_folder,subtitle_language)
					succ_num+=1
					total_file+=succ
	
				except: 
					fail_link+=1
	logging.info("{}/{} links are successfully downloaded, {} links failed and {} files in total are downloaded".format(succ_num,link_num,fail_link,total_file))


def main(_):
	#load input_file and parse file_name, link
	logging.getLogger().setLevel(logging.INFO)

	input_file=FLAGS.input_file
	link=FLAGS.link
	filename=FLAGS.filename
	output_folder=FLAGS.output_folder
	subtitle_language=FLAGS.subtitle_language
	download_audio(input_file,link,filename,output_folder,subtitle_language)

		

	
if __name__ == "__main__":

	app.run(main)
