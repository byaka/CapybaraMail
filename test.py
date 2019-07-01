# -*- coding: utf-8 -*-
from functionsex import *

from VombatiDB import VombatiDB, showDB, showStats, Workspace
from VombatiDB import errors as dbError

from importMail import ImportMailMBox
from store import StoreBase, StoreFilesLocal, StoreDB, StoreDB_dialogFinderEx
import errors as storeError
import api

from libs.plainText import plaintext
import textwrap

class MyEnv(object):
   def __init__(self):
      self._istty=console.inTerm()
      self._autoLabel_inbox='Inbox'
      self.workspace=Workspace()
      self.store=ClassFactory(StoreBase, (StoreFilesLocal, StoreDB, StoreDB_dialogFinderEx))(self.workspace)
      self.api=ClassFactory(api.ApiBase, (api.ApiAccaunt, api.ApiLabel, api.ApiFilter))(self.workspace, store=self.store)
      self.store.start()
      self.api.start()

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

   def importData(self, user, path, skip=0):
      if not os.path.isfile(path):
         raise ValueError('File not exists')
      self.store.userIsExist(user,needException=True)
      importer=ImportMailMBox(path, skip)
      msg=None
      if self._istty:
         print
         msg='%(clearLast)sImporting from %(bold)s'+path+'%(end)s: %%i'
         msg=msg%console.color
      for i, (msgObj, headers, body, attachments) in enumerate(importer):
         try:
            isIncoming=self.store._msgProc_isIncoming(user, headers, msgObj.raw, True)
         except storeError.IncorrectInputError: continue
         if isIncoming:
            self.addMsgIncoming(user, msgObj, body, headers, attachments)
         else:
            self.addMsgOutgoing(user, msgObj, body, headers, attachments)
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
         self.workspace.log(2, 'Some defects founded in msg: \t\n%s\n'%
            '\t\n'.join(
               '%s:\n%s'%(k, '\t\t\n'.join(o)) for k, o in msgObj.defects.iteritems()
            )
         )
      labels=labels or ()
      if extractMoreLabels:
         if headers.get('x-gmail-labels'):
            labels+=headers['x-gmail-labels']
      try:
         self.store.msgAdd(user, body, headers, msgObj.raw, attachments=attachments, labels=labels, strictMode=True, allowCompress=True)
      except storeError.NoMessageIdError:
         print '='*30
         print 'ERROR: no msg-id founded'
         print msgObj.raw
         print '='*30
         print

   def show(self, branch=None, limit=None):
      showDB(self.store.db, branch=branch, limit=limit)

   def stats(self):
      showStats(self.store.db)

   def __call__(self):
      assert self._istty
      scope=globals().copy()
      scope.update((k, getattr(self, k)) for k in dir(self) if not k.startswith('_'))
      console.interact(scope)

   def test_dialogs(self, min_msgs=2):
      for idsDialog, _ in self.store.db.iterBranch((self.store.userId('John Smith'), 'node_dialog'), recursive=False):
         for idsDialogLinked, _ in self.store.db.iterBacklinks(idsDialog, recursive=False):
            lines=['DIALOG (%s)'%idsDialog[-1]]
            n=len(idsDialogLinked)
            for idsMsg,_ in self.store.db.iterBranch(idsDialogLinked):
               data=self.store.db.get(idsMsg)
               lines.append('%s%s [%s] `%s`'%(
                  '   '*(len(idsMsg)-n),
                  '>>' if data.isIncoming else '<<',
                  data.timestamp,
                  data.subject
               ))
               body=data.bodyPlain or plaintext(data.bodyHtml, linebreaks=1, indentation=False)
               body=textwrap.wrap(body, 100)
               s='   '*(1+len(idsMsg)-n)
               body='\n'.join(s+line for line in body)
               lines.append(body)
               lines.append('%s%s'%(
                  '   '*(len(idsMsg)-n+1),
                  '='*40
               ))
            if len(lines)>=1+min_msgs*3:
               print '\n'.join(lines)



if __name__ == '__main__':
   # importer=ImportMailMBox('/home/byaka/Загрузки/gmail_exported/all.mbox')
   # tMap=set()
   # i1=i2=i3=i4=0
   # print
   # for _, headers, (body_plain, body_html), attachments in importer:
   #    if headers.get('message-id'):
   #       if headers['message-id'] in tMap: i4+=1
   #       tMap.add(headers['message-id'])
   #    else:
   #       i2+=1
   #    i1+=1
   #    if headers.get('in-reply-to') in tMap: i3+=1

   #    print console.color.clearLast, i1, i2, i3, i4

   #    if not headers.get('message-id'):
   #       print _.raw
   #       print '='*30
   #       print

   #    continue

   #    for k in importer._headers:
   #       print k+':', strUniDecode('%r'%(headers[k],))
   #    print
   #    # print body_plain or body_html
   #    print
   #    for o in attachments:
   #       o=o.copy()
   #       o['payload']='...'
   #       print o
   #    print '='*40
   #    print _.defects, raw_input()
   # print console.color.clearLast, i1, i2, i3, i4, sys.exit()

   # q={
   #    'or':[
   #       {'key':'label', 'value':'label1', 'match':'=='},
   #       {'and':[
   #          {'key':'label', 'value':'label2', 'match':'!='},
   #          {'or':[
   #             {'key':'from', 'value':'from1', 'match':'=='},
   #             {'key':'from', 'value':'from2', 'match':'=='},
   #             {'and':[
   #                {'key':'label', 'value':'label3', 'match':'!='},
   #                {'key':'from', 'value':'from3', 'match':'=='},
   #             ]},
   #             {'key':'label', 'value':'label4', 'match':'=='},
   #          ]},
   #       ]},
   #       {'key':'from', 'value':'from4', 'match':'=='},
   #       {'key':'from', 'value':'from5', 'match':'=='},
   #    ]
   # }
   # MyEnv().store.dialogFindEx('John Smith', q)
   # sys.exit(0)

   o=MyEnv()

   # o.show((u'user#john_smith', u'node_date', u'date#20170412', u'node_email'))

   # for ids in o.store.db.getLinked((u'user#john_smith', u'node_date', u'date#20170412', u'node_email', u'email#mail@ajon.ru')):
   #    print ids
   # print '*'
   # ids=o.store.msgFind_byMsg('John Smith', '075D1B0F-A99A-4377-B99A-9E059870D327@ajon.ru', date=None, strictMode=True)
   # for ids in o.store.db.getBacklinks(ids):
   #    print ids

   for date, data, targets in o.api.filterMessages('John Smith',
      dates=('today', '-1', True),
      query={'or':[
         {'key':'from', 'value':'mail@ajon.ru', 'match':'=='},
      ]}, limitDates=2, limitResults=10, asDialogs=True, returnFull=True):
      print date
      print_r(data)
      print targets
      print '='*30
