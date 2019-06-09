# -*- coding: utf-8 -*-
from functionsex import *

from VombatiDB import VombatiDB, showDB, showStats, Workspace
from VombatiDB import errors as dbError

from importMail import ImportMailMBox
from store import StoreBase, StoreFilesLocal, StoreDB
from errors import StoreError, AccessDeniedError

if __name__ == '__main__':
   # mailStore=ClassFactory(StoreBase, (StoreFilesLocal, StoreDB))(Workspace())
   # mailStore.start()

   importer=ImportMailMBox('/home/byaka/Загрузки/gmail_exported/inbox.mbox')
   for _, headers, (body_plain, body_html), attachments in importer:
      for k in importer._headers:
         print k.upper()+':', decode_utf8('%s'%headers[k])
      print
      print body_plain
      print
      print body_html
      print
      for o in attachments:
         o=o.copy()
         o['payload']='...'
         print o
      print '='*40
      print _.defects, raw_input()
