# -*- coding: utf-8 -*-
from functionsex import *

from VombatiDB import VombatiDB, showDB, showStats, Workspace
from VombatiDB import errors as dbError

from importMail import ImportMailMBox
from store import StoreBase, StoreFilesLocal, StoreDB
from errors import StoreError, AccessDeniedError

class MyEnv(object):
   def __init__(self):
      self.workspace=Workspace()
      self.store=ClassFactory(StoreBase, (StoreFilesLocal, StoreDB))(self.workspace)
      self.store.start()

   def listUsers(self):
      p=console.color.copy()
      tpl='%(enabled)s%(bold)s%(name)s%(end)s (%(descr)s)' if console.inTerm() else '%(name)s (%(descr)s)'
      if console.inTerm(): print '-'*40
      for n,o in self.store.userList():
         print tpl%dict(
            p.items()+o.items(),
            name=n,
            enabled=(console.color.green, console.color.red)[o.isActive]
         )
      if console.inTerm(): print '-'*40

   def addUser(self, user, password, descr=None, avatar=None):
      if avatar:
         raise NotImplementedError
      self.store.userAdd(user, password, descr=descr, avatar=avatar, strictMode=True)

   def importData(user, path):
      if not os.path.isfile(path):
         raise ValueError('File not exists')
      self.store.userIsExist(user, True)
      importer=ImportMailMBox(path)
      for o, headers, (body_plain, body_html), attachments in importer:
         if o.defects:
            self.workspace.log(2, 'Some defects founded in msg: %r'%o.defects)
         self.store.msgAdd(user, body_html, headers, o.raw, attachments=attachments, label=None, strictMode=True, allowCompressRaw=True)

   def show(self):
      showDB(self.store.db)

   def __call__(self):
      assert console.inTerm()
      scope={k:getattr(self, k) for k in dir(self) if not k.startswith('_')}
      console.interact(scope)


if __name__ == '__main__':
   # importer=ImportMailMBox('/home/byaka/Загрузки/gmail_exported/inbox.mbox')
   # for _, headers, (body_plain, body_html), attachments in importer:
   #    for k in importer._headers:
   #       print k.upper()+':', decode_utf8('%s'%headers[k])
   #    print
   #    print body_plain
   #    print
   #    print body_html
   #    print
   #    for o in attachments:
   #       o=o.copy()
   #       o['payload']='...'
   #       print o
   #    print '='*40
   #    print _.defects, raw_input()
   MyEnv()()
