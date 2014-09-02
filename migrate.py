# -*- coding:utf-8 -*-

# using sqlalchemy mysql-connector

import sqlalchemy
from sqlalchemy.orm import sessionmaker

from db.models import WordSnapshot

DATABASE_USER = "root"
DATABASE_PASS = "123456"
DATABASE_HOST = "127.0.0.1"
DATABASE_NAME = "duofilter"

engineStat    = sqlalchemy.create_engine('mysql+mysqlconnector://%s:%s@%s:3306/%s?charset=utf8' % (DATABASE_USER, DATABASE_PASS, DATABASE_HOST, DATABASE_NAME), echo = False)

SessionStat   = sessionmaker(bind = engineStat)
session_stat  = SessionStat(autoflush = True)

# *<args><cr><lf>
# $<len><cr><lf>
# <arg0><cr><lf>
# <arg1><cr><lf>
# ...
# <argN><cr><lf>

def gen_redis_word(word, comment_count, word_count, word_id):
	proto = str()

	proto += "*5\r\n"

	proto += "$5\r\n"
	proto += "rpush\r\n"

	proto += "$" + str( len( bytearray(word, "utf-8") )) + "\r\n"
	proto += str(word) + "\r\n"

	proto += "$" + str( len( str(comment_count) ) ) + "\r\n"
	proto += str(comment_count) + "\r\n"

	proto += "$" + str( len( str(word_count) ) ) + "\r\n"
	proto += str(word_count) + "\r\n"

	proto += "$" + str( len( str(word_id)) ) + "\r\n"
	proto += str(word_id) + "\r\n"

	print(proto, end = "")

def gen_redis_count(comment_count, word_count):
	proto = str()

	proto += "*3\r\n"

	proto += "$" + str( len("set") ) + "\r\n"
	proto += "set\r\n"

	proto += "$" + str( len( "comment_count" )) + "\r\n"
	proto += "comment_count" + "\r\n"

	proto += "$" + str( len( str(comment_count) )) + "\r\n"
	proto += str(comment_count) + "\r\n"

	proto += "*3\r\n"

	proto += "$" + str( len("set") ) + "\r\n"
	proto += "set\r\n"

	proto += "$" + str( len( "word_count" )) + "\r\n"
	proto += "word_count" + "\r\n"

	proto += "$" + str( len( str(word_count) )) + "\r\n"
	proto += str(word_count) + "\r\n"

	print(proto, end = "")

READ_LIMIT = 100000
TOT_LIMIT  = 1450000 

def process():
	st = 0
	while st < TOT_LIMIT:
		q = session_stat.query(WordSnapshot).offset(st).limit(READ_LIMIT)
		session_stat.commit()
		for i in q:
			if i.getWordid() == 0:
				gen_redis_count(i.getCommentcount(), i.getWordcount())
			else:
				gen_redis_word(i.getWord(), i.getCommentcount(), i.getWordcount(), i.getWordid())
		st += READ_LIMIT


if __name__ == '__main__':
	process()