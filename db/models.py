# -*- coding:utf-8 -*-

from sqlalchemy import Table, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

WORD_TABLE_NAME = 'worddic'
class WordSnapshot(Base):
    __tablename__ = WORD_TABLE_NAME

    word_id            = Column(Integer, primary_key=True)
    comment_count      = Column(Integer)
    word_count         = Column(Integer)
    word               = Column(String)

    def __init__(self, **params):
        self.word_id        = params['word_id']
        self.comment_count  = params['comment_count']
        self.word_count     = params['word_count']
        self.word           = params['word']

    def __repr__(self):
        return "<Word(%d, %d, %d, '%s')>" % (self.word_id, self.comment_count, self.word_count, self.word)

    def getWordid(self):
        if int(self.word_id) >= 0:
            return self.word_id
        else:
            return None

    def getCommentcount(self):
        if int(self.comment_count) >= 0:
            return self.comment_count
        else:
            return None

    def getWordcount(self):
        if int(self.word_count) >= 0:
            return self.word_count
        else:
            return None

    def getWord(self):
        if self.word:
            return self.word
        else:
            return None
