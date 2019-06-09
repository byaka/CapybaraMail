# -*- coding: utf-8 -*-
from functionsex import *
from VombatiDB import VombatiDB
from VombatiDB import errors as dbError

from scheme import SCHEME
from errors import StoreError, AccessDeniedError

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
      self._settings=dict(self.settings)
      self.settings._MagicDictCold__freeze()
      self._supports=dict(self.supports)
      self.supports._MagicDictCold__freeze()
      self.settings_frozen=True
      self._start(**kwargs)
      self.started=True

   def _start(self, **kwargs):
      pass

class StoreFilesBase(StoreBase):
   def _init(self, **kwargs):
      super(StoreFilesBase, self)._init(**kwargs)
      self.supports.file=True
      self.supports.file_sanitize=True

   def _fileSanitize(self, name):
      assert isinstance(name, (str, unicode))
      if isinstance(name, unicode):
         try:
            name=name.encode("utf-8")
         except Exception: pass
      return base64.urlsafe_b64decode(name)

   def _fileSet(self, name, content, allowOverwrite=False):
      raise NotImplementedError

   def _fileGet(self, name, nameEncoded=True, asGenerator=False):
      raise NotImplementedError

import base64
class StoreFilesLocal(StoreFilesBase):
   def _init(self, path=None, **kwargs):
      self.settings.path=path or os.path.join(getScriptPath()+'files')
      if not os.path.exists(self.settings.path):
         os.makedirs(self.settings.path)
      super(StoreFilesLocal, self)._init(**kwargs)

   def _fileSet(self, name, content, allowOverwrite=False):
      assert isinstance(content, str)
      fn=self._fileSanitize(name)
      fp=os.path.join(self._settings['path'], fn)
      if os.path.exists(fp) and not allowOverwrite:
         raise IOError('File already exists')  #! fix
      with open(fp, 'r') as f:
         f=self.workspace.fileWrap(f)
         f.write(content)
      return fn

   def _fileGet(self, name, nameEncoded=True, asGenerator=False):
      if nameEncoded: fn=name
      else:
         fn=self._fileSanitize(name)
      fp=os.path.join(self._settings['path'], fn)
      if not os.path.exists(fp):
         raise IOError('File not exists')  #! fix
      def tFunc_gen(f):
         for l in f: yield l
      with open(fp, 'r') as f:
         f=self.workspace.fileWrap(f)
         if asGenerator:
            return tFunc_gen(f)
         else:
            return f.read()

class StoreDB(StoreBase):
   def _init(self, **kwargs):
      if not hasattr(self.workspace, 'dbPath'):
         #! это жестко привязывает класс к использованию расширения `StorePersistentWithCache`
         self.workspace.dbPath=getScriptPath(real=True, f=__file__)+'/db'
      self.settings.reinitNamespacesOnStart=True
      self.db=None
      super(StoreDB, self)._init(**kwargs)

   def _start(self, **kwargs):
      self.db=VombatiDB(('NS', 'Columns', 'MatchableLinks', 'StorePersistentWithCache', 'Search'))(self.workspace, self.workspace.dbPath)
      self._configureDB(reinitNamespaces=self._settings['reinitNamespacesOnStart'])
      super(StoreDB, self)._start(**kwargs)

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

   @staticmethod
   def userId(user):
      if isinstance(user, str):
         if user.startswith('user#'): return user
         else:
            return 'user#%s'%user
      raise ValueError('Incorrect type')

   @staticmethod
   def dateId(date):
      if isinstance(date, str) and date.startswith('date#'): return date
      elif isinstance(date, (datetime.date, datetime.datetime)):
         return 'date#'+date.strftime('%Y%m%d')
      raise ValueError('Incorrect type')

   @staticmethod
   def emailId(email):
      if isinstance(email, str):
         if email.startswith('email#'): return email
         else:
            return 'email#%s'%email
      raise ValueError('Incorrect type')

   @staticmethod
   def dialogId(index):
      if isinstance(index, str) and index.startswith('dialog#'): return index
      elif isinstance(index, int):
         return 'dialog#%s'%index
      raise ValueError('Incorrect type')

   def userAdd(self, user, password, descr=None, avatar=None, strictMode=True):
      assert isStr(password) and password
      userId=self.userId(user)
      if self.db.isExist(userId):
         raise LogicError(-106)
      passwordHash=password  #! здесь добавить хеширование
      data={
         '_passwordHash':passwordHash,
         '_connector':None,
         'isActive':True,
         'name':user,
         'descr':descr,
         'avatar':avatar,
      }
      self.db.set(userId, data, strictMode=strictMode, onlyIfExist=False)
      self.db.set((userId, 'node_date'), False, strictMode=False, onlyIfExist=False)
      self.db.set((userId, 'node_email'), False, strictMode=False, onlyIfExist=False)
      self.db.set((userId, 'node_label'), False, strictMode=False, onlyIfExist=False)
      return (userId,)

   def dateAdd(self, user, date, strictMode=False):
      userId=self.userId(user)
      dateId=self.dateId(date)
      self.db.set((userId, 'node_date', dateId), False, strictMode=strictMode, onlyIfExist=False)
      self.db.set((userId, 'node_date', dateId, 'node_email'), False, strictMode=False, onlyIfExist=False)
      self.db.set((userId, 'node_date', dateId, 'node_dialog'), False, strictMode=False, onlyIfExist=False)
      self.db.set((userId, 'node_date', dateId, 'node_msg'), False, strictMode=False, onlyIfExist=False)
      return (userId, 'node_date', dateId)

   def emailAdd(self, user, email, name=None, strictMode=False):
      userId=self.userId(user)
      emailId=self.emailId(email)
      self.db.set((userId, 'node_email', emailId), False, strictMode=strictMode, onlyIfExist=False)
      return (userId, 'node_email', emailId)

   def dialogAdd(self, user, date, strictMode=False):
      userId=self.userId(user)
      dateId=self.dateId(date)
      ids=self.db.set((userId, 'node_date', dateId, 'node_dialog', 'dialog+'), False, strictMode=strictMode)
      return ids

   def msgAdd(self, user, isIncoming, msg, body, params, raw, label=None, strictMode=True):
      userId=self.userId(user)
      _linkTo=[]
      data={
         'subject':params['subject'],
         'timestamp':params['timestamp'],
         'isIncoming':bool(isIncoming),
         'raw':raw,
         'body':body,
         'attachments':None,
      }
      #
      dateIds=self.dateAdd(userId, params['timestamp'], strictMode=strictMode)
      msgIds=dateIds+('node_msg', self.msgId(msg))
      #
      isFrom=True
      for k in ('from', 'to', 'cc', 'bcc'):
         v=params.get(k) or None
         if isFrom:
            data[k], v=v, (v,)
         else:
            data[k]=tuple(_v[1] for _v in v) if v else None
         for vName, vEmail in v:
            emailIds=self.emailAdd(user, vEmail, vName, strictMode=strictMode)
            emailIds=self.db.link(
               dateIds+('node_email', emailIds[-1]),
               emailIds, strictMode=strictMode, onlyIfExist=False)
            _linkTo.append(emailIds)
         isFrom=False
      #
      replyTo=params.get('in-reply-to')
      dialogIds=self.msgGet_byId(user, replyTo) if replyTo else False
      if dialogIds is False:
         dialogIds=self.dialogAdd(userId, dateIds[-1])
      _linkTo.append(dialogIds)
      #
      if params.get('attachments'):
         if not self._supports.get('file'):
            self.workspace.log(2, 'Saving files not supported')
         else:
            pass  #! fixme
      #
      self.db.set(msgIds, data, strictMode=strictMode, onlyIfExist=False)
      for ids in _linkTo:
         self.db.link(
            ids+(msgIds[-1]),
            msgIds, strictMode=strictMode, onlyIfExist=False)
      #
      #? большой вопрос - как хранить вложенные лэйблы. подход gmail кажется весьма неплохим
      # if label:
      #    toNow=
      #    for l in label:
      return msgIds

   def msgGet_byId(self, user, msg, date=NULL):
      #? можно ускорить для несуществующих в индексе добавив отдельный фильтр блума
      userId=self.userId(user)
      idsSuf=('node_msg', self.msgId(msg))
      msgIds=None
      if date is NULL:
         for ids, (props, l) in self.db.iterBranch((userId, 'node_date'), recursive=False, safeMode=False, calcProperties=False, skipLinkChecking=True, allowContextSwitch=False):
            ids+=idsSuf
            if self.db.isExist(ids):
               msgIds=ids
               break
      else:
         ids=(userId, 'node_date', self.dateId(date))+idsSuf
         if self.db.isExist(ids):
            msgIds=ids
      #
      if msgIds is None: return False
      for ids, _ in self.db.iterBacklink(msgIds, recursive=False, safeMode=False, calcProperties=False, strictMode=True, allowContextSwitch=False):
         if len(ids)>4 and ids[3]=='node_dialog': return ids[:5]
      raise RuntimeError('Msg founded but no link to dialog')  #! fixme

   # def userEdit(self, user, descr=NULL, avatar=NULL):
   #    userId=self.userId(user)
   #    if descr is NULL and avatar is NULL: return
   #    data={}
   #    if descr is not NULL: data['descr']=descr
   #    if avatar is not NULL: data['avatar']=avatar
   #    try:
   #       self.db.set(userId, data, allowMerge=True, strictMode=True, onlyIfExist=True)
   #    except dbError.ExistStatusMismatchError:
   #       raise LogicError(-104)
