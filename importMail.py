# -*- coding: utf-8 -*-
from functionsex import *

import mailparserEx

class ImportMailMBox(object):
   _headers=('date', 'from', 'to', 'cc', 'bcc', 'message-id', 'in-reply-to', 'references', 'reply-to', 'archived-at', 'sender', 'x-gmail-labels', 'subject', 'delivered-to', 'return-path')

   def __init__(self, path, skip=0):
      self.path=path
      self.skip=skip
      self._headers_preprocess={
         'message-id':lambda s: s.replace('<', '').replace('>', ''),
         'in-reply-to':lambda s: s.replace('<', '').replace('>', ''),
         'references':lambda s: s.replace('<', '').replace('>', ''),
         'x-gmail-labels':self._parseLabels_GMail,
      }

   @classmethod
   def _parseLabels_GMail(cls, data):
      return tuple(
         (tuple(
            ss.strip() for ss in s.split('/')
         ) if '/' in s else s.strip())
         for s in data.split(',')
      )

   def _procMsg(self, buffer):
      if not buffer: return None
      s=''.join(buffer)
      # fileWrite('/home/byaka/Загрузки/gmail_exported/test.txt', s)
      msg=mailparserEx.parse_from_string(s)
      headers={}
      for k in self._headers:
         headers[k]=getattr(msg, k.replace('-', '_'))
         if headers[k] and k in self._headers_preprocess:
            m=self._headers_preprocess[k]
            headers[k]=m(headers[k])
      body=msg.body
      attachments=tuple(msg.attachments)
      return msg, headers, body, attachments

   def __iter__(self):
      i=0
      with open(self.path, 'rb') as f:
         buffer=[]
         for line in f:
            if not line.startswith('From '): buffer.append(line)
            elif buffer:
               i+=1
               if i>self.skip:
                  yield self._procMsg(buffer)
               buffer*=0  #~ in PY2 no `list.clear()` but this is same
         r=self._procMsg(buffer)
         if r: yield r
