# -*- coding: utf-8 -*-
from functionsex import *

from VombatiDB import VombatiDB, showDB, showStats, Workspace
from VombatiDB import errors as dbError

from importMail import ImportMailMBox
from store import StoreBase, StoreFilesLocal, StoreDB
from errors import StoreError, AccessDeniedError

class MyEnv(object):
   def __init__(self):
      self._istty=console.inTerm()
      self._autoLabel_inbox='Inbox'
      self.workspace=Workspace()
      self.store=ClassFactory(StoreBase, (StoreFilesLocal, StoreDB))(self.workspace)
      self.store.start()

   def listUsers(self):
      p=console.color.copy()
      tpl='%(enabled)s%(bold)s%(name)s%(end)s (%(descr)s)' if self._istty else '%(name)s (%(descr)s)'
      if self._istty: print '-'*40
      for n,o in self.store.userList():
         print tpl%dict(
            p.items()+o.items(),
            name=n,
            enabled=(console.color.green, console.color.red)[o.isActive]
         )
      if self._istty: print '-'*40

   def addUser(self, user, password, email, descr=None, avatar=None):
      if avatar:
         raise NotImplementedError
      self.store.userAdd(user, password, descr=descr, avatar=avatar, strictMode=True)
      for s in ((email,) if isinstance(email, (str, unicode)) else email):
         self.store.userSelfEmailAdd(user, s, name=user, strictMode=False)

   def importData(self, user, path):
      if not os.path.isfile(path):
         raise ValueError('File not exists')
      self.store.userIsExist(user,needException=True)
      importer=ImportMailMBox(path)
      msg=None
      if self._istty:
         print
         msg='%(clearLast)sImporting from %(bold)s'+path+'%(end)s: %%i'
         msg=msg%console.color
      for i, (msgObj, headers, (_, body), attachments) in enumerate(importer):
         self.addMsgIncoming(user, msgObj, body, headers, attachments)
         if self._istty:
            print msg%(i+1)
            # raw_input('Success! Continue?')
            # print console.color.clearLast

   def addMsgIncoming(self, user, msgObj, body, headers, attachments):
      labels=(self._autoLabel_inbox,)
      self._addMsg(user, msgObj, body, headers, attachments, labels, extractMoreLabels=True)

   def addMsgOutgoing(self, user, msgObj, body, headers, attachments):
      self._addMsg(user, msgObj, body, headers, attachments, None, extractMoreLabels=True)

   def _addMsg(self, user, msgObj, body, headers, attachments, labels, extractMoreLabels=True):
      if msgObj.defects:
         print msgObj.defects
         # self.workspace.log(2, 'Some defects founded in msg: %s\n'%
         #    '   \n'.join('%s:\n%s'%(k, '      \n'.join(o)) for k, o in msgObj.defects.iteritems())
         # )
      labels=labels or ()
      if extractMoreLabels:
         if headers.get('x-gmail-labels'):
            labels+=headers['x-gmail-labels']
      self.store.msgAdd(user, body, headers, msgObj.raw, attachments=attachments, labels=labels, strictMode=True, allowCompress=True)

   def show(self):
      showDB(self.store.db)

   def __call__(self):
      assert self._istty
      scope={k:getattr(self, k) for k in dir(self) if not k.startswith('_')}
      console.interact(scope)


if __name__ == '__main__':
   # importer=ImportMailMBox('/home/byaka/Загрузки/gmail_exported/all.mbox')
   # for _, headers, (body_plain, body_html), attachments in importer:
   #    if not headers['references']: continue

   #    for k in importer._headers:
   #       print k.upper()+':', strUniDecode('%r'%(headers[k],))
   #    print
   #    print body_plain or body_html
   #    print
   #    for o in attachments:
   #       o=o.copy()
   #       o['payload']='...'
   #       print o
   #    print '='*40
   #    print _.defects, raw_input()
   MyEnv()()
