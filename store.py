# -*- coding: utf-8 -*-
from functionsex import *
from VombatiDB import VombatiDB
from VombatiDB import errors as dbError

from scheme import SCHEME
from errors import *

class StoreBase(object):
   def __init__(self, workspace, **kwargs):
      self._main_app=sys.modules['__main__']
      self.inited=False
      self.started=False
      self.settings_frozen=False
      self.workspace=workspace
      self.settings=self._settings=MagicDictCold({})
      self.supports=self._supports=MagicDictCold({})
      self._init(**kwargs)
      self._inited(**kwargs)

   def _init(self, **kwargs):
      pass

   def _inited(self, **kwargs):
      self.inited=True

   def start(self, **kwargs):
      if self.started: return
      self._settings=dict(self.settings)
      self.settings._MagicDictCold__freeze()
      self._supports=dict(self.supports)
      self.supports._MagicDictCold__freeze()
      self.settings_frozen=True
      self._start(**kwargs)
      self.started=True

   def _start(self, **kwargs):
      pass

import base64
class StoreFilesBase(StoreBase):
   def _init(self, **kwargs):
      super(StoreFilesBase, self)._init(**kwargs)
      self.supports.file=True
      self.supports.file_sanitize=True

   def _fileNameNormalize(self, name):
      assert isinstance(name, (str, unicode))
      if isinstance(name, unicode):
         try:
            name=name.encode("utf-8")
         except Exception: pass
      return base64.urlsafe_b64encode(name)

   def _fileSet(self, name, content, nameNormalized=False, allowOverwrite=False):
      raise NotImplementedError

   def _fileGet(self, name, nameNormalized=True, asGenerator=False):
      raise NotImplementedError

class StoreFilesLocal(StoreFilesBase):
   def _init(self, fileStorePath=None, **kwargs):
      self.settings.fileStorePath=fileStorePath or os.path.join(getScriptPath(real=True, f=__file__), 'files')
      if not os.path.exists(self.settings.fileStorePath):
         os.makedirs(self.settings.fileStorePath)
      super(StoreFilesLocal, self)._init(**kwargs)

   def _fileSet(self, name, content, nameNormalized=False, allowOverwrite=False, strictMode=False):
      assert isinstance(content, (str, unicode))
      # content=content.encode('utf-8')
      if nameNormalized: fn=name
      else:
         fn=self._fileNameNormalize(name)
      fp=os.path.join(self._settings['fileStorePath'], fn)
      if os.path.exists(fp) and not allowOverwrite:
         if strictMode:
            raise IOError('File already exists')  #! fix
         else: return fn
      with open(fp, 'wb') as f:
         f=self.workspace.fileWrap(f)
         f.write(content)
      return fn

   def _fileGet(self, name, nameNormalized=True, asGenerator=False):
      if nameNormalized: fn=name
      else:
         fn=self._fileNameNormalize(name)
      fp=os.path.join(self._settings['fileStorePath'], fn)
      if not os.path.exists(fp):
         raise IOError('File not exists')  #! fix
      def tFunc_gen(f):
         for l in f: yield l
      with open(fp, 'rb') as f:
         f=self.workspace.fileWrap(f)
         if asGenerator:
            return tFunc_gen(f)
         else:
            return f.read()

class StoreDB(StoreBase):
   def _init(self, **kwargs):
      if not hasattr(self.workspace, 'dbPath'):
         #! это жестко привязывает класс к использованию расширения `StorePersistentWithCache`
         self.workspace.dbPath=os.path.join(getScriptPath(real=True, f=__file__), 'db')
      self.settings.reinitNamespacesOnStart=True
      self.db=None
      super(StoreDB, self)._init(**kwargs)

   def _start(self, **kwargs):
      self.db=VombatiDB(('NS', 'Columns', 'MatchableLinks', 'StorePersistentWithCache', 'Search'))(self.workspace, self.workspace.dbPath)
      self._configureDB(reinitNamespaces=self._settings['reinitNamespacesOnStart'])
      super(StoreDB, self)._start(**kwargs)

   def _configureDB(self, reinitNamespaces):
      self.db.settings.store_flushOnChange=False
      self.db.settings.ns_checkIndexOnConnect=False
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

   @staticmethod
   def userId(user):
      if isinstance(user, (str, unicode)):
         if user.startswith('user#'): return user
         else:
            #! нужен более изящный способ замены
            return 'user#%s'%user.lower().replace(' ', '_').replace('?', '?_').replace('+', '+_')
      raise ValueError('Incorrect type')

   @staticmethod
   def dateId(date):
      if isinstance(date, (str, unicode)) and date.startswith('date#'): return date
      elif isinstance(date, (datetime.date, datetime.datetime)):
         return 'date#'+date.strftime('%Y%m%d')
      raise ValueError('Incorrect type')

   @staticmethod
   def emailId(email):
      if isinstance(email, (str, unicode)):
         if email.startswith('email#'): return email
         else:
            return 'email#%s'%email
      raise ValueError('Incorrect type')

   @staticmethod
   def dialogId(dialog):
      if isinstance(dialog, (str, unicode)) and dialog.startswith('dialog#'): return dialog
      elif isinstance(dialog, int):
         return 'dialog#%s'%dialog
      raise ValueError('Incorrect type')

   @staticmethod
   def labelId(label):
      if isinstance(label, (str, unicode)):
         if label.startswith('label#'): return label
         else:
            #! нужен более изящный способ замены
            return 'label#%s'%label.lower().replace(' ', '_').replace('?', '?_').replace('+', '+_')
      raise ValueError('Incorrect type')

   @staticmethod
   def msgId(msg):
      if isinstance(msg, (str, unicode)):
         if msg.startswith('msg#'): return msg
         else:
            return 'msg#%s'%msg
      raise ValueError('Incorrect type')

   @staticmethod
   def problemId(problem):
      if isinstance(problem, (str, unicode)):
         if problem.startswith('problem#'): return problem
         else:
            #! нужен более изящный способ замены
            return 'problem#%s'%problem.lower().replace(' ', '_').replace('?', '?_').replace('+', '+_')
      raise ValueError('Incorrect type')

   def userAdd(self, user, password, descr=None, avatar=None, strictMode=True):
      assert user and isinstance(user, (str, unicode))
      assert password and isinstance(password, (str, unicode))
      assert isinstance(descr, (str, unicode, types.NoneType))
      assert isinstance(avatar, (str, unicode, types.NoneType))
      #
      if avatar and not self._supports.get('file'):
         raise NotSupportedError('Cant save avatar')
      userId=self.userId(user)
      if self.db.isExist(userId):
         raise StoreError(-106)
      passwordHash=password  #! здесь добавить хеширование
      if avatar:
         s='%s_avatar'%self._fileNameNormalize(userId)
         avatar=self._fileSet(s, avatar, allowOverwrite=False, strictMode=True)
      data={
         '_passwordHash':passwordHash,
         'isActive':True,
         'name':user,
         'descr':descr or None,
         'avatar':avatar or None,
      }
      ids=self.db.set(userId, data, strictMode=strictMode, onlyIfExist=False)
      self.db.set((userId, 'node_self'), False, strictMode=False, onlyIfExist=False)
      self.db.set((userId, 'node_date'), False, strictMode=False, onlyIfExist=False)
      self.db.set((userId, 'node_email'), False, strictMode=False, onlyIfExist=False)
      self.db.set((userId, 'node_label'), False, strictMode=False, onlyIfExist=False)
      self.db.set((userId, 'node_dialog'), False, strictMode=False, onlyIfExist=False)
      self.db.set((userId, 'node_problem'), False, strictMode=False, onlyIfExist=False)
      return ids

   def userIsExist(self, user, needException=False):
      userId=self.userId(user)
      r=self.db.isExist(userId)
      if needException and not r:
         raise ValueError('User not exists')  #! fix
      return r

   def userSelfEmailAdd(self, user, email, name=None, strictMode=False):
      userId=self.userId(user)
      emailId=self.emailId(email)
      emailIds=self.emailAdd(userId, emailId, name=name, strictMode=False)
      ids=(userId, 'node_self', emailId)
      self.db.link(ids, emailIds, strictMode=strictMode, onlyIfExist=False)
      return ids

   def userSelfEmailCheck(self, user, email):
      userId=self.userId(user)
      emailId=self.emailId(email)
      ids=(userId, 'node_self', emailId)
      return ids if self.db.isExist(ids) else False

   def problemAdd(self, user, problem, descr=None, strictMode=False):
      userId=self.userId(user)
      problemId=self.problemId(problem)
      data={
         'name':problem,
         'descr':descr,
      }
      return self.db.set((userId, 'node_problem', problemId), data, strictMode=strictMode, onlyIfExist=False)

   def dateAdd(self, user, date, strictMode=False):
      userId=self.userId(user)
      dateId=self.dateId(date)
      ids=self.db.set((userId, 'node_date', dateId), False, strictMode=strictMode, onlyIfExist=False)
      self.db.set((userId, 'node_date', dateId, 'node_email'), False, strictMode=False, onlyIfExist=False)
      self.db.set((userId, 'node_date', dateId, 'node_dialog'), False, strictMode=False, onlyIfExist=False)
      self.db.set((userId, 'node_date', dateId, 'node_msg'), False, strictMode=False, onlyIfExist=False)
      return ids

   def emailAdd(self, user, email, name=None, strictMode=False):
      userId=self.userId(user)
      emailId=self.emailId(email)
      return self.db.set((userId, 'node_email', emailId), False, strictMode=strictMode, onlyIfExist=False)

   def dialogAdd(self, user, strictMode=True):
      userId=self.userId(user)
      return self.db.set((userId, 'node_dialog', 'dialog+'), False, strictMode=strictMode, onlyIfExist=False)

   def labelAdd(self, user, label, descr=None, color=None, strictMode=False):
      if not label:
         raise ValueError  #! fix
      userId=self.userId(user)
      label=label if isinstance(label, tuple) else (label,)
      labelIds=tuple(self.labelId(l) for l in label)
      idsPref=(userId, 'node_label',)
      data={'name':None, 'descr':None, 'color':None}
      for i in xrange(len(labelIds)):
         if i==len(labelIds)-1:
            data={'descr':descr, 'color':color}
         data['name']=label[i]
         ids=self.db.set(idsPref+labelIds[:i+1], data, strictMode=strictMode, onlyIfExist=False)
      return ids

   def _msgProc_isIncoming(self, user, headers, raw, strictMode):
      try:
         s=headers['from'][0][1]
      except (IndexError, KeyError):
         print '='*30
         print 'ERROR: Cant read `from` header'
         print raw
         print '='*30
         print
         raise IncorrectInputError('Cant read `from` header')
      userId=self.userId(user)
      return not(self.userSelfEmailCheck(userId, s))
      #! нужна также проверка, есть ли пользователь в адресатах и доп-хак на случай, если письмо было отправлено самому себе

   def _msgProc_members(self, user=NULL, dateIds=NULL, data=NULL, linkToMsg=NULL, headers=NULL, **kwargs):
      headers['to']=(headers.get('to') or [])+(headers.get('delivered-to') or [])
      _need_unpack=set((
         'from',
         'replyTo',
         'returnPath'
      ))
      for i, k in enumerate((
         'from', 'to', 'cc', 'bcc',
         ('reply-to', 'replyTo'),
         ('return-path', 'returnPath'),
      )):
         if isinstance(k, tuple): kk, k=k
         else: kk=k
         v=headers.get(kk) or None
         if not v:
            data[k]=None
         elif k in _need_unpack:
            data[k]=v[0][1]
         else:
            data[k]=tuple(_v[1] for _v in v)
         #
         for vName, vEmail in v or ():
            if not vEmail: continue
            ids=self.emailAdd(user, vEmail, vName, strictMode=False)
            ids=self.db.link(
               dateIds+('node_email', ids[-1]),
               ids, strictMode=False, onlyIfExist=False)
            linkToMsg.append(ids)

   def _msgProc_dialog(self, headers=NULL, user=NULL, userId=NULL, dateIds=NULL, linkToMsg=NULL, linkInMsg=NULL, **kwargs):
      replyTo=headers.get('in-reply-to')
      if not replyTo:
         replyTo=headers.get('references') or ''
         replyTo=tuple(s.strip() for s in replyTo.split(' ') if s.strip())
         if replyTo: replyTo=replyTo[-1]
      ids=self.dialogFind_byMsg(user, replyTo, asThread=True) if replyTo else False
      if ids is False:
         if replyTo:

            problemIds=self.problemAdd(userId, 'Parent message missed')
            linkInMsg.append((problemIds[-1], problemIds))
         #
         ids=self.dialogAdd(userId)
         ids=self.db.link(
            dateIds+('node_dialog', ids[-1]),
            ids, strictMode=False, onlyIfExist=False)
      linkToMsg.append(ids)

   def _msgProc_attachments(self, msg=NULL, attachments=NULL, strictMode=NULL, data=NULL, **kwargs):
      if attachments:
         if not self._supports.get('file'):
            self.workspace.log(2, 'Saving files not supported')
         else:
            n=self._fileNameNormalize(msg)
            for i, o in enumerate(attachments):
               name='%s_%i'%(n, i+1)
               content=o.pop('payload')
               if o['binary']:
                  content=base64.b64decode(content)
               else:
                  try:
                     content=content.encode("utf-8")
                  except Exception: pass
               o.pop('binary', None)
               o.pop('content_transfer_encoding', None)
               o['_store_fileId']=self._fileSet(name, content, nameNormalized=True, allowOverwrite=not(strictMode), strictMode=False)
         data['attachments']=tuple(attachments) if not isinstance(attachments, tuple) else attachments

   def _msgProc_labels(self, userId=NULL, labels=NULL, linkInMsg=NULL, **kwargs):
      if labels:
         for label in labels:
            ids=self.labelAdd(userId, label, strictMode=False)
            linkInMsg.append((ids[-1], ids))

   def msgAdd(self, user, body, headers, raw, labels=None, attachments=None, msg=None, strictMode=True, allowCompress=True):
      msg=msg or headers.get('message-id')
      if not msg:
         raise NoMessageIdError()
      if isinstance(msg, int): msg=str(msg)
      assert isinstance(msg, (str, unicode))
      assert isinstance(body, tuple) and len(body)==2
      #
      isIncoming=self._msgProc_isIncoming(user, headers, raw, strictMode)
      bodyPlain, bodyHtml=body
      data={
         'subject':headers['subject'],
         'timestamp':headers['date'],
         'isIncoming':isIncoming,
         '_raw':'',  #! очень много места занимают, хорошо бы хранить их в файлах
         'bodyPlain':bodyPlain,
         'bodyHtml':bodyHtml,
         'replyTo':None,
         'attachments':None,
      }
      #
      linkInMsg=[]
      linkToMsg=[]
      userId=self.userId(user)
      dateIds=self.dateAdd(userId, headers['date'], strictMode=False)
      msgIds=dateIds+('node_msg', self.msgId(msg))
      #
      p=locals()
      del p['self']
      self._msgProc_members(**p)
      self._msgProc_dialog(**p)
      self._msgProc_attachments(**p)
      self._msgProc_labels(**p)
      #
      try:
         self.db.set(msgIds, data, strictMode=strictMode, onlyIfExist=False)
      except dbError.ExistStatusMismatchError:
         #~ проверяем, если у письма в получателях более одного нашего ящика, то это просто дубликат пришедший на альтернативную почту.
         #~ для верности также проверяем все поля кроме timestamp, body* и _raw
         print '='*30
         print 'ERROR: msg-id already exists'
         print msgIds
         print '='*30
         print
         return False

      for ids in linkToMsg:
         self.db.link(
            ids+(msgIds[-1],),
            msgIds, strictMode=False, onlyIfExist=False)
      for idSuf, ids in linkInMsg:
         self.db.link(
            msgIds+(idSuf if isinstance(idSuf, tuple) else (idSuf,)),
            ids, strictMode=False, onlyIfExist=False)
      return msgIds

   def dialogFind_byMsg(self, user, msg, date=None, asThread=False, strictMode=False):
      msgId=self.msgId(msg)
      ids=self.msgFind_byMsg(user, msgId, date, strictMode=strictMode)
      if ids is None: return False
      g=self.db.iterBacklinks(ids, recursive=False, safeMode=False, calcProperties=False, strictMode=True, allowContextSwitch=False)
      for ids, _ in g:
         if len(ids)>4 and ids[3]=='node_dialog' and ids[-1]==msgId:
            return ids if asThread else ids[:5]
      raise RuntimeError('Msg founded but no link to dialog')  #! fixme

   def dialogGet(self, user, dialog, date=None, strictMode=False, returnProps=False):
      if date is None:
         ids=(self.userId(user), 'node_dialog', self.dialogId(dialog))
         try:
            g=self.db.iterBacklinks(ids, recursive=False, safeMode=False, calcProperties=False, strictMode=True, allowContextSwitch=False)
         except dbError.NotExistError:
            if not strictMode: return ()
            raise
         for ids, _ in g:
            if len(ids)==5 and ids[3]=='node_dialog': break
         else:
            raise RuntimeError('Inconsistency')  #! fix
      else:
         ids=(self.userId(user), 'node_date', self.dateId(date), 'node_dialog', self.dialogId(dialog))
         if not self.db.isExist(ids):
            if not strictMode: return ()
            else:
               raise dbError.NotExistError(ids)
      #
      return self.dialogGet_byIds(ids, returnProps=returnProps)

   def dialogGet_byIds(self, ids, props=None, returnProps=False):
      g=self.db.iterBranch(ids, recursive=True, treeMode=True, safeMode=False, calcProperties=returnProps, skipLinkChecking=True, allowContextSwitch=False)
      for ids, (props, l) in g:
         yield (ids, props) if returnProps else ids

   def msgGet(self, user, msg, date=None, strictMode=False, onlyPublic=True, resolveAttachments=True, andLabels=True):
      ids=self.msgFind_byMsg(user, msg, date, strictMode=strictMode)
      if not ids: return None
      return self.msgGet_byIds(ids, strictMode=strictMode, onlyPublic=onlyPublic, resolveAttachments=resolveAttachments, andLabels=andLabels)

   def msgFind_byMsg(self, user, msg, date=None, strictMode=False):
      #? можно ускорить для несуществующих в индексе добавив отдельный фильтр блума
      userId=self.userId(user)
      msgId=self.msgId(msg)
      idsSuf=('node_msg', msgId)
      targetIds=None
      if not date:
         for ids, (props, l) in self.db.iterBranch((userId, 'node_date'), recursive=False, safeMode=False, calcProperties=False, skipLinkChecking=True, allowContextSwitch=False):
            ids+=idsSuf
            if self.db.isExist(ids):
               targetIds=ids
               break
      else:
         ids=(userId, 'node_date', self.dateId(date))+idsSuf
         if self.db.isExist(ids):
            targetIds=ids
      if targetIds is None:
         if not strictMode: return None
         else:
            raise dbError.NotExistError(ids)
      return targetIds

   def msgGet_byIds(self, ids, props=None, strictMode=True, onlyPublic=False, resolveAttachments=False, andLabels=False):
      if andLabels:
         # получение данных сработает и по ссылке, но для лэйблов нужна прямая адресация
         ids=self.db.resolveLink(ids)
      #
      o=self.db.get(ids, existChecked=props, returnRaw=True, strictMode=strictMode)
      if not strictMode and o is None:
         return {}
      res={k:v for k,v in o.iteritems() if k[0]!='_'} if onlyPublic else o.copy()
      o['id']=ids[-1]
      if resolveAttachments:
         pass  #! fix
      if andLabels:
         #! после VombatiDB#99 можно будет передавать `branch` через аргумент `arg` и таким образом кешировать запрос
         res['labels']=tuple(self.db.query(
            branch=ids,
            what='INDEX',
            where='NS=="label"',
            recursive=False,
         ))
      return res

   def userList(self, filterPrivateData=True, wrapMagic=True):
      g=self.db.query(
         what='INDEX, DATA',
         where='NS=="user"',
         recursive=False,
      )
      for name, data in g:
         if filterPrivateData:
            data={k:v for k,v in data.iteritems() if not k.startswith('_')}
         if wrapMagic:
            data=MagicDictCold(data)
            data._MagicDictCold__freeze()
         yield name, data

import textwrap
class StoreDB_dialogFinderEx(StoreDB):
   __finderMatchMap={
      '==':'&',
      '!=':'-',
   }

   def __queryCompile_forKey(self, toPre, toIter, counter, userId, key, value, match):
      if match not in self.__finderMatchMap:
         raise ValueError('Unknown matching pattern')  #! fix
      #! добавить проверку на пустой set
      if key=='label':
         counter['CHECK_WITH_label']+=1
         var='CHECK_WITH_label%i'%counter['CHECK_WITH_label']
         toPre("%s=db_getBacklinks(('%s', 'node_label', '%s'), strictMode=False, safeMode=False)"%(var, userId, self.labelId(value)))
      elif key=='from':
         counter['CHECK_WITH_from']+=1
         var='CHECK_WITH_from%i'%counter['CHECK_WITH_from']
         toPre("%s=set()"%var)
         toPre("g=db_iterBacklinks(('%s', 'node_email', '%s'), recursive=False, safeMode=False, calcProperties=False, strictMode=False, allowContextSwitch=False)"%(userId, self.emailId(value)))
         toPre("%s.update(*(db_getLinked(ids, strictMode=False, safeMode=False) for ids, _ in g if len(ids)==5 and ids[1]=='node_date'))"%var)
      elif key=='to':
         raise NotImplementedError
      elif key=='unreaded':
         raise NotImplementedError
      elif key=='date':
         raise NotImplementedError
      else:
         raise ValueError('Unknown key')  #! fix
      toIter("CURR_PART %s= %s  # OBJ.%s %s '%s'"%(self.__finderMatchMap[match], var, key, match, value))

   def __queryCompile_forOp(self, toPre, toIter, counter, userId, op, conds):
      if not isinstance(conds, (list, tuple, types.GeneratorType)):
         raise ValueError('Incorrect section in query: %r'%conds)
      if not conds: return
      #! переписать без рекурсии
      if op is None:
         toIter('if CURR_PART:')
         tO=[]
         toIter(tO)
         _toIter=tO.append
         #
         for o in conds:
            if 'key' in o:
               self.__queryCompile_forKey(toPre, _toIter, counter, userId, o['key'], o['value'], o['match'])
            elif len(o)==1:
               self.__queryCompile_forOp(toPre, _toIter, counter, userId, *next(o.iteritems()))
      elif op=='and':
         toIter('# AND <<')
         #
         for o in conds:
            if 'key' in o:
               self.__queryCompile_forKey(toPre, toIter, counter, userId, o['key'], o['value'], o['match'])
            elif len(o)==1:
               self.__queryCompile_forOp(toPre, toIter, counter, userId, *next(o.iteritems()))
         #
         toIter('# >> AND')
      elif op=='or':
         toIter('# OR <<')
         counter['CURR_PART_BCK']+=1
         CURR_PART_BCK='CURR_PART_BCK%i'%counter['CURR_PART_BCK']
         toIter(CURR_PART_BCK+'=CURR_PART.copy()')
         _toIter=toIter
         #
         for i, o in enumerate(conds):
            if i:
               _toIter('if not CURR_PART:')
               tO=[]
               _toIter(tO)
               _toIter=tO.append
            if i:
               _toIter('CURR_PART='+CURR_PART_BCK)
            if 'key' in o:
               self.__queryCompile_forKey(toPre, _toIter, counter, userId, o['key'], o['value'], o['match'])
            elif len(o)==1:
               self.__queryCompile_forOp(toPre, _toIter, counter, userId, *next(o.iteritems()))
         #
         toIter('# >> OR')
      else:
         raise ValueError  #! fix

   def __queryCompile(self, userId, query):
      _tab=' '*3
      pre=[]
      onIter=[]
      counter=defaultdict(int)
      self.__queryCompile_forOp(pre.append, onIter.append, counter, userId, None, (query,))
      #
      qRaw='{"custom":"StoreDB_dialogFinderEx", "query":%r, "limit":None}'%query
      onIter.append('if CURR_PART: yield date, CURR_PART')
      #
      code=["""
         def RUN():
            try:
               db_getLinked=DB.getLinked
               db_get=DB.get
               db_iterBranch=DB.iterBranch
               db_getBacklinks=DB.getBacklinks
               db_iterBacklinks=DB.iterBacklinks
               # PRE <<
               %s
               # >> PRE
               cAll=cDays=0
               gDates=DATES
               for date, dateId in gDates:
                  IDS=('%s', 'node_date', dateId, 'node_msg')
                  CURR_PART=set(ids for ids, _ in db_iterBranch(IDS, strictMode=False, recursive=False, safeMode=False, calcProperties=False, skipLinkChecking=True, allowContextSwitch=False))"""%(
            ('\n'+_tab*5).join(self.db._indentMultilineSource(_tab, pre)),
            userId),
         ('\n'+_tab*6)+('\n'+_tab*6).join(self.db._indentMultilineSource(_tab, onIter)),
         """
            except Exception: __QUERY_ERROR_HANDLER(RUN.source, RUN.query)
         RUN.query=%s"""%(qRaw),
      ]
      #
      code=('').join(code)
      code=textwrap.dedent(code)
      # code=fileGet('filter_source.py')
      # fileWrite('filter_source.py', code)
      code+='\nRUN.source="""%s"""'%code
      code=compile(code, self.db.query_envName, 'exec')
      #~ сейчас в code полностью сформированный исходник, однако генератор дат он берет из окружения - значит можно смело кешировать и переиспользовать с другими датами
      return code

   def __queryDateIter(self, dates):
      assert dates
      assert isinstance(dates, (list, tuple))
      _ptrnStr=(str, unicode)
      _ptrnDate=datetime.date
      _ptrnDatetime=datetime.datetime
      _fromStr=datetime.datetime.strptime
      _fromInt=datetime.date.fromtimestamp
      _today=datetime.date.today()
      _epo=_ptrnDate(1970, 1, 1)
      _delta=datetime.timedelta
      _yesterday=_today-_delta(days=1)
      #! нужно получать максимальную и минимальную дату в базе и использовать это
      _dateId=self.dateId
      old=None
      step=None
      for s in dates:
         if not s:
            raise NotImplementedError  #! fix
         elif isinstance(s, _ptrnStr):
            s=s.lower()
            if s=='today':
               d=_today
            elif s=='yesterday':
               d=_yesterday
            elif len(s)>1 and (s[0]=='+' or s[0]=='-'):
               assert old and step is None
               step=(s[0]=='+', int(s[1:]))
               continue
            else:
               d=_fromStr(s, '%Y%m%d').date()
         elif s is not True and s is not False and isinstance(s, int):
            d=_fromInt(s)
         elif isinstance(s, _ptrnDate):
            d=s
         elif isinstance(s, _ptrnDatetime):
            d=s.date()
         elif s is True and step is not None:
            d=_today if step[0] else _epo
         else:
            raise IncorrectInputError('Incorrect date value `%s`: %s'%(s, e))
         #
         if step is None:
            yield (d, _dateId(d))
         elif old==d:
            raise IncorrectInputError('Passed dateStart and dateEnd must not equal')
         elif (old>d)==step[0]:
            raise IncorrectInputError('Incorrect dateStart and dateEnd')
         else:
            r=step[0]
            delta=(1 if r else -1)*_delta(days=step[1])
            end, d=d, old+delta
            while (d<=end if r else d>=end):
               yield (d, _dateId(d))
               d+=delta
            step=None
         old=d

   def dialogFindEx(self, user, query, dates):
      userId=self.userId(user)
      dateIterator=self.__queryDateIter(dates)
      q=self.__queryCompile(userId, query)
      return self.db.query(q=q, env={'DATES':dateIterator})
