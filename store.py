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

import base64
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
      return base64.urlsafe_b64encode(name)

   def _fileSet(self, name, content, allowOverwrite=False):
      raise NotImplementedError

   def _fileGet(self, name, nameEncoded=True, asGenerator=False):
      raise NotImplementedError

class StoreFilesLocal(StoreFilesBase):
   def _init(self, fileStorePath=None, **kwargs):
      self.settings.fileStorePath=fileStorePath or os.path.join(getScriptPath(real=True, f=__file__), 'files')
      if not os.path.exists(self.settings.fileStorePath):
         os.makedirs(self.settings.fileStorePath)
      super(StoreFilesLocal, self)._init(**kwargs)

   def _fileSet(self, name, content, allowOverwrite=False, strictMode=False):
      assert isinstance(content, (str, unicode))
      fn=self._fileSanitize(name)
      fp=os.path.join(self._settings['fileStorePath'], fn)
      if os.path.exists(fp) and not allowOverwrite:
         if strictMode:
            raise IOError('File already exists')  #! fix
         else: return fn
      with open(fp, 'wb') as f:
         f=self.workspace.fileWrap(f)
         f.write(content)
      return fn

   def _fileGet(self, name, nameEncoded=True, asGenerator=False):
      if nameEncoded: fn=name
      else:
         fn=self._fileSanitize(name)
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
   def dialogId(index):
      if isinstance(index, (str, unicode)) and index.startswith('dialog#'): return index
      elif isinstance(index, int):
         return 'dialog#%s'%index
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

   def userAdd(self, user, password, descr=None, avatar=None, strictMode=True):
      assert isStr(password) and password
      userId=self.userId(user)
      if self.db.isExist(userId):
         raise StoreError(-106)
      passwordHash=password  #! здесь добавить хеширование
      #! аватрку нужно записывать в fileStore и оставлять лишь ссылку
      data={
         '_passwordHash':passwordHash,
         'isActive':True,
         'name':user,
         'descr':descr,
         'avatar':avatar,
      }
      ids=self.db.set(userId, data, strictMode=strictMode, onlyIfExist=False)
      self.db.set((userId, 'node_self'), False, strictMode=False, onlyIfExist=False)
      self.db.set((userId, 'node_date'), False, strictMode=False, onlyIfExist=False)
      self.db.set((userId, 'node_email'), False, strictMode=False, onlyIfExist=False)
      self.db.set((userId, 'node_label'), False, strictMode=False, onlyIfExist=False)
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

   def dialogAdd(self, user, date, strictMode=False):
      userId=self.userId(user)
      dateId=self.dateId(date)
      return self.db.set((userId, 'node_date', dateId, 'node_dialog', 'dialog+'), False, strictMode=strictMode)

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
      if 'from' in headers:
         userId=self.userId(user)
         s=headers['from'][0][1]
         return not(self.userSelfEmailCheck(userId, s))
         #! нужна также проверка, есть ли пользователь в адресатах и допо-хак на случай, если письмо было отправлено самому себе
      else:
         raise ValueError('No `from` field')  #! fix

   def _msgProc_members(self, user=NULL, dateIds=NULL, data=NULL, linkToMsg=NULL, headers=NULL, **kwargs):
      headers['to']=(headers.get('to') or [])+(headers.get('delivered-to') or [])
      _need_unpack=set(('from', 'replyTo', 'returnPath'))
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
         for vName, vEmail in v or ():
            if not vEmail: continue
            ids=self.emailAdd(user, vEmail, vName, strictMode=False)
            ids=self.db.link(
               dateIds+('node_email', ids[-1]),
               ids, strictMode=False, onlyIfExist=False)
            linkToMsg.append(ids)

   def _msgProc_dialog(self, headers=NULL, user=NULL, userId=NULL, dateIds=NULL, linkToMsg=NULL, **kwargs):
      replyTo=headers.get('in-reply-to')
      if not replyTo:
         replyTo=headers.get('references') or ''
         replyTo=tuple(s.strip() for s in replyTo.split(' ') if s.strip())
         if replyTo: replyTo=replyTo[-1]
      ids=self.msgGet_byId(user, replyTo) if replyTo else False
      if ids is False:
         ids=self.dialogAdd(userId, dateIds[-1])
      linkToMsg.append(ids)

   def _msgProc_attachments(self, attachments=NULL, strictMode=NULL, data=NULL, **kwargs):
      if attachments:
         if not self._supports.get('file'):
            self.workspace.log(2, 'Saving files not supported')
         else:
            for o in attachments:
               name=o['filename']
               content=o.pop('payload')
               if o['binary']:
                  content=base64.b64decode(content)
               o.pop('binary', None)
               o.pop('content_transfer_encoding', None)
               o['_store_fileId']=self._fileSet(name, content, allowOverwrite=not(strictMode), strictMode=False)
         data['attachments']=tuple(attachments) if not isinstance(attachments, tuple) else attachments

   def _msgProc_labels(self, userId=NULL, labels=NULL, linkInMsg=NULL, **kwargs):
      if labels:
         for label in labels:
            ids=self.labelAdd(userId, label, strictMode=False)
            linkInMsg.append((ids[-1], ids))

   def msgAdd(self, user, body, headers, raw, labels=None, attachments=None, msg=None, strictMode=True, allowCompress=True):
      msg=msg or headers.get('message-id')
      assert msg and isinstance(msg, (str, unicode))
      isIncoming=self._msgProc_isIncoming(user, headers, raw, strictMode)
      data={
         'subject':headers['subject'],
         'timestamp':headers['date'],
         'isIncoming':isIncoming,
         'raw':'',  #! очень много места занимают, хорошобы хранить их в файлах
         'body':body,
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
      self.db.set(msgIds, data, strictMode=strictMode, onlyIfExist=False)
      for ids in linkToMsg:
         self.db.link(
            ids+(msgIds[-1],),
            msgIds, strictMode=False, onlyIfExist=False)
      for idSuf, ids in linkInMsg:
         self.db.link(
            msgIds+(idSuf if isinstance(idSuf, tuple) else (idSuf,)),
            ids, strictMode=False, onlyIfExist=False)
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
