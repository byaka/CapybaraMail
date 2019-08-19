# -*- coding: utf-8 -*-
from functionsex import *

from VombatiDB import VombatiDB, showDB, showStats, Workspace
from VombatiDB import errors as dbError

from importMail import ImportMail_MBox
import errors as storeError
import api
from utils import RepairDialogLinking

from libs.plainText import plaintext
import textwrap

class MyEnv(object):
   def __init__(self):
      self._istty=console.inTerm()
      self._autoLabel_inbox='Inbox'
      self.workspace=Workspace()
      self.store=api.makeStoreClass()(self.workspace)
      self.api=ClassFactory(api.ApiBase, (
         api.ApiAccount,
         api.ApiLabel,
         api.ApiFilter,
      ))(self.workspace, store=self.store)
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
      importer=ImportMail_MBox(path, skip)
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

   def repairDialogs(self, user):
      RepairDialogLinking(self.store).run(user)

   def show(self, branch=None, limit=None):
      showDB(self.store.db, branch=branch, limit=limit)

   def stats(self):
      showStats(self.store.db)

   def __call__(self):
      assert self._istty
      scope=globals().copy()
      scope.update((k, getattr(self, k)) for k in dir(self) if not k.startswith('_'))
      console.interact(scope)

   def test_filter(self, q, dates=None, limitDates=2, limitResults=10, asDialogs=True, returnFull=True):
      data, targets, nextDates=o.api.filterMessages('John Smith', dates=dates, query=q, limitDates=limitDates, limitResults=limitResults, asDialogs=asDialogs, returnFull=returnFull)
      for date, data in data:
         print date
         # body=data['bodyPlain'] or plaintext(data['bodyHtml'], linebreaks=1, indentation=False)
         # del data['bodyHtml']
         # del data['bodyPlain']
         print_r(data)
         print '-'*30
      print 'TARGETS =', targets
      print 'NEXT_DATES =', nextDates
      print '='*30

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

   def find_broken_msgs_without_dialogs(self, user):
      """ Изза ошибки в коде `utils.RepairDialogLinking` у некоторых сообщений пропадал линк на диалог. Данный тест искал такие сломанные сообщения."""
      i1=i2=0
      tArr=[]
      g=self.store.db.iterBranch((self.store.userId(user), 'node_date'), strictMode=True, recursive=True, treeMode=True, safeMode=False, calcProperties=False, skipLinkChecking=True)
      for ids, (props, l) in g:
         if len(ids)<4: continue
         if ids[3]!='node_msg': g.send(False)  # skip not-msgs nodes
         if len(ids)>5: g.send(False)  # skip branch inside msgs
         if len(ids)==5:
            try:
               self.store.dialogFind_byMsgIds(ids, strictMode=True, asThread=True)
               i1+=1
            except Exception:
               tArr.append(ids)
               i2+=1
         print console.color.clearLast+'%i  %i'%(i1, i2)
      print tArr

if __name__ == '__main__':
   # importer=ImportMail_MBox('/home/byaka/Загрузки/gmail_exported/all.mbox')
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

   o=MyEnv()

   o.repairDialogs('John Smith')

   # o.test_filter({'or':[
   #    {'key':'from', 'value':'mail@ajon.ru', 'match':'=='},
   #    # {'key':'label', 'value':u'черновики', 'match':'=='},
   # ]}, asDialogs=True, returnFull=False, limitDates=30, limitResults=100)

   o()
