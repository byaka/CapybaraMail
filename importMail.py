# -*- coding: utf-8 -*-
from functionsex import *

import mailbox, email

class ImportMailMBoxOld(object):
   _headers=('date', 'from', 'to', 'cc', 'bcc', 'message-id', 'in-reply-to', 'references', 'reply-to', 'archived-at', 'sender', 'x-gmail-labels', 'subject')

   def __init__(self, path):
      self.path=path
      self._headers_preprocess={
         'from':self._parseAddress,
         'to':self._parseAddress,
         'cc':self._parseAddress,
         'bcc':self._parseAddress,
         'date':email.utils.parsedate_tz,
         'subject':self._decodeHeader,
         'x-gmail-labels':self._parseLabels_GMail,
      }

   @classmethod
   def _parseAddress(cls, data):
      n,v=email.utils.parseaddr(data)
      return (cls._decodeHeader(n), v)

   @classmethod
   def _parseLabels_GMail(cls, data):
      return tuple(
         (tuple(
            ss.strip() for ss in s.split('/')
         ) if '/' in s else s.strip())
         for s in cls._decodeHeader(data).split(',')
      )

   @classmethod
   def _decodeText(cls, obj):
      t=obj.get_content_type()
      if t=='text/plain' or t=='text/html':
         return obj.get_payload(decode=True)
      else:
         print '! unknown type', t
         return False

   @classmethod
   def getBody(cls, message):
      if message.is_multipart():
         for part in message.walk():
            if part.is_multipart():
               for subpart in part.walk():
                  data=cls._decodeText(subpart)
                  if data is not False:
                     return data
            else:
               data=cls._decodeText(part)
               if data is not False:
                  return data
      else:
         data=cls._decodeText(message)
         if data is not False:
            return data
      return None

   @classmethod
   def _decodeHeader(cls, data):
      # data=re.sub(r"(=\?.*\?=)(?!$)", r"\1 ", data)  # fix broken headers, https://stackoverflow.com/a/7331577
      res=''.join(unicode(s, e or 'ASCII') for s,e in email.header.decode_header(data))
      return res

   def __iter__(self):
      for message in mailbox.mbox(self.path):
         attachments=[]
         headers={
            'charset':message.get_charset(),
         }
         # https://en.wikipedia.org/wiki/Email#Header_fields
         for k in self._headers:
            headers[k]=message.get_all(k)
            if headers[k] and k in self._headers_preprocess:
               m=self._headers_preprocess[k]
               headers[k]=tuple(m(v) for v in headers[k])
         body=self.getBody(message)
         #
         yield message, headers, body, attachments



import mailparser

# class MailParserFixed(MailParser):
#    @property
#    def body(self):
#       return self.text_plain, self.text_html

class ImportMailMBox(object):
   _headers=('date', 'from', 'to', 'cc', 'bcc', 'message-id', 'in-reply-to', 'references', 'reply-to', 'archived-at', 'sender', 'x-gmail-labels', 'subject')
   _headers=tuple(k.replace('-', '_') for k in _headers)

   def __init__(self, path):
      self.path=path
      self._headers_preprocess={}

   def __iter__(self):
      with open(self.path, 'rb') as f:
         buffer=[]
         for line in f:
            if not line.startswith('From '): buffer.append(line)
            else:
               if buffer:
                  s, buffer=''.join(buffer), None
                  # fileWrite('/home/byaka/Загрузки/gmail_exported/test.txt', s)
                  msg=mailparser.parse_from_string(s)
                  headers={}
                  for k in self._headers:
                     headers[k]=getattr(msg, k+'')
                     if not isinstance(headers[k], tuple):
                        headers[k]=(headers[k],)
                     if headers[k] and k in self._headers_preprocess:
                        m=self._headers_preprocess[k]
                        headers[k]=tuple(m(v) for v in headers[k])
                  body=(
                     ''.join(msg.text_plain),
                     ''.join(msg.text_html),
                  )
                  attachments=tuple(msg.attachments)
                  yield msg, headers, body, attachments
               buffer=[]
