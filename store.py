# -*- coding: utf-8 -*-
from functionsex import *
from VombatiDB import VombatiDB, showDB, showStats, Workspace
from VombatiDB import errors as dbError

from scheme import SCHEME
from errors import CapybaraMailError, AccessDeniedError

class Store(object):
   def __init__(self, workspace=None, reinitNamespaces=True):
      self.workspace=workspace or Workspace()
      if not hasattr(self.workspace, 'dbPath'):
         self.workspace.dbPath=getScriptPath(real=True, f=__file__)+'/db'
      self.db=VombatiDB(('NS', 'Columns', 'MatchableLinks', 'StorePersistentWithCache', 'Search'))(self.workspace, self.workspace.dbPath)
      self._configureDB(reinitNamespaces=reinitNamespaces)

   def _configureDB(self, reinitNamespaces):
      self.db.settings.store_flushOnChange=False
      self.db.settings.ns_checkIndexOnConnect=True
      self.db.settings.dataMerge_ex=True
      self.db.settings.dataMerge_deep=False
      self.db.settings.linkedChilds_default_do=False
      self.db.settings.linkedChilds_inheritNSFlags=True
      self.db.settings.ns_default_allowLocalAutoIncrement=False
      self.db.settings.columns_default_allowUnknown=False
      self.db.settings.columns_default_allowMissed=False
      self.db.connect()
      if reinitNamespaces:
         self.db.configureNS(SCHEME, andClear=True)


import mailbox, email

class ImportMBox(object):

   _headers=('date', 'from', 'to', 'cc', 'bcc', 'message-id', 'in-reply-to', 'references', 'reply-to', 'archived-at', 'sender', 'x-gmail-labels', 'subject')

   def __init__(self, path):
      self.path=path
      self._headers_preprocess={
         'from':email.utils.parseaddr,  #! у адресов первая часть тоже может быть закодирована
         'to':email.utils.parseaddr,
         'cc':email.utils.parseaddr,
         'bcc':email.utils.parseaddr,
         'date':email.utils.parsedate_tz,
         'subject':self._decodeHeader,
         'x-gmail-labels':self._decodeHeader,
      }

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
         yield message, headers, body


if __name__ == '__main__':
   importer=ImportMBox('/home/byaka/Загрузки/gmail_exported/inbox.mbox')
   for _, headers, body in importer:
      for k in importer._headers:
         print k.upper()+':', decode_utf8('%s'%headers[k])
      print
      print body
      print '='*40
      raw_input()
