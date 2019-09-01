# -*- coding: utf-8 -*-
from functionsex import *
from VombatiDB import VombatiDB
from VombatiDB import errors as dbError

from scheme import DB_SCHEME, DB_SETTINGS
from errors import *
from utils import isInt

re_prepForId=re.compile(r'[^\w_]', re.U)

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
      self._supports=defaultdict(bool, **self.supports)
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

   def _fileSet(self, prefix, name, content, nameNormalized=False, allowOverwrite=False):
      raise NotImplementedError

   def _fileGet(self, prefix, name, nameNormalized=True, asGenerator=False):
      raise NotImplementedError

class StoreFilesLocal(StoreFilesBase):
   def _init(self, fileStorePath=None, **kwargs):
      self.settings.fileStorePath=fileStorePath or os.path.join(getScriptPath(real=True, f=__file__), 'files')
      if not os.path.exists(self.settings.fileStorePath):
         os.makedirs(self.settings.fileStorePath)
      super(StoreFilesLocal, self)._init(**kwargs)

   def _fileSet(self, prefix, name, content, nameNormalized=False, allowOverwrite=False, strictMode=False):
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

   def _fileGet(self, prefix, name, nameNormalized=True, asGenerator=False):
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

class StoreHashing_dummy(StoreBase):
   def passwordHash(self, password):
      return password

class StoreUtils(StoreBase):
   def _init(self, **kwargs):
      self.JT_prepDataForReturn={
         (s1, s2):bind(self.prepDataForReturn, defaultsUpdate={'filterPrivateData':s1, 'wrapMagic':s2})
         for s1, s2 in itertools.product((True, False), repeat=2)
      }
      super(StoreUtils, self)._init(**kwargs)

   @staticmethod
   def prepDataForReturn(data, filterPrivateData=True, wrapMagic=True):
      if filterPrivateData:
         data={k:v for k,v in data.iteritems() if not k.startswith('_')}
      if wrapMagic:
         data=MagicDictCold(data)
         data._MagicDictCold__freeze()
      return data

   def ids2human(self, ids):
      if ids and isinstance(ids, tuple): ids=ids[-1]
      assert ids and isinstance(ids, (str, unicode))
      ns, nsi=self.db._parseId2NS(ids, needNSO=False)
      return nsi[1:]

class StoreDB(StoreBase):
   def _init(self, **kwargs):
      self.___specialLabel=[
         'unread',
      ]
      if not hasattr(self.workspace, 'dbPath'):
         #! это жестко привязывает класс к использованию расширения `StorePersistentWithCache`
         self.workspace.dbPath=os.path.join(getScriptPath(real=True, f=__file__), 'db')
      self.settings.reinitNamespacesOnStart=True
      self.supports.specialLabel_unread=True
      self.db=None
      self.__authMap=None
      super(StoreDB, self)._init(**kwargs)

   def _start(self, **kwargs):
      self.db=VombatiDB(('NS', 'Columns', 'MatchableLinks', 'StorePersistentWithCache', 'Search'))(self.workspace, self.workspace.dbPath)
      self._configureDB(reinitNamespaces=self._settings['reinitNamespacesOnStart'])
      self.__makeAuthMap()
      super(StoreDB, self)._start(**kwargs)

   def _configureDB(self, reinitNamespaces):
      for k,v in DB_SETTINGS.iteritems(): self.db.settings[k]=v
      self.db.connect()
      if reinitNamespaces:
         self.db.configureNS(DB_SCHEME, andClear=True)

   def __makeAuthMap(self):
      self.__authMap=dict(self.db.query(
         what='IDS[-1], DATA["_passwordHash"]',
         recursive=False,
      ))

   @staticmethod
   def userId(user):
      if isinstance(user, (str, unicode)):
         if user.startswith('user#'): return user
         else:
            return u'user#%s'%re_prepForId.sub('_', user.lower())
      raise ValueError('Incorrect type')

   @staticmethod
   def dateId(date):
      if isinstance(date, (str, unicode)) and date.startswith('date#'): return date
      elif isinstance(date, (datetime.date, datetime.datetime)):
         return u'date#'+date.strftime('%Y%m%d')
      raise ValueError('Incorrect type')

   @staticmethod
   def emailId(email):
      if isinstance(email, (str, unicode)):
         if email.startswith('email#'): return email
         else:
            return u'email#%s'%email
      raise ValueError('Incorrect type')

   @staticmethod
   def dialogId(dialog):
      if isinstance(dialog, (str, unicode)):
         if dialog.startswith('dialog#'): return dialog
         else:
            try: dialog=int(dialog)
            except Exception: pass
      if isInt(dialog):
         return u'dialog#%s'%dialog
      raise ValueError('Incorrect type')

   @staticmethod
   def labelId(label):
      if isinstance(label, (str, unicode)):
         if label.startswith('label#'): return label
         else:
            return u'label#%s'%re_prepForId.sub('_', label.lower())
      raise ValueError('Incorrect type')

   @staticmethod
   def msgId(msg):
      if isinstance(msg, (str, unicode)):
         if msg.startswith('msg#'): return msg
         else:
            return u'msg#%s'%msg
      raise ValueError('Incorrect type')

   @staticmethod
   def problemId(problem):
      if isinstance(problem, (str, unicode)):
         if problem.startswith('problem#'): return problem
         else:
            return u'problem#%s'%re_prepForId.sub('_', problem.lower())
      raise ValueError('Incorrect type')

   def labelIds2human(self, ids):
      assert isinstance(ids, tuple)
      assert len(ids)>2 and ids[1]=='node_label'
      return '/'.join(self.ids2human(s) for s in ids[2:])

   def dialogIds2human(self, ids):
      assert isinstance(ids, tuple)
      assert len(ids)>3 and ids[1]=='node_date' and ids[3]=='node_dialog'
      _date=self.db._parseId2NS(ids[2], needNSO=False)[1][1:]
      _dialog=self.db._parseId2NS(ids[4], needNSO=False)[1][1:]
      return '%s:%s'%(_date, _dialog)

   def _idsConv_thread2dialog(self, ids, onlyDialog=False):
      for i, s in enumerate(ids):
         ns, nsi=self.db._parseId2NS(s, needNSO=False)
         if ns=='dialog':
            return nsi[1:] if onlyDialog else ids[:i+1]
      return None

   def _idsConv_dialog2date(self, ids, onlyRaw=False, onlyDate=False):
      assert isinstance(ids, tuple)
      assert len(ids)>3 and ids[1]=='node_date' and ids[3]=='node_dialog'
      ns, nsi=self.db._parseId2NS(ids[2], needNSO=False)
      assert ns=='date'
      if onlyRaw:
         return datetime.datetime.strptime(nsi[1:], '%Y%m%d')
      return nsi[1:] if onlyDate else ids[:3]

   def authMap(self):
      if self.__authMap is None:
         raise ValueError('Auth-map not inited yet')  #! fix
      return self.__authMap

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
      passwordHash=self.passwordHash(password)
      if avatar:
         s='%s_avatar'%self._fileNameNormalize(userId)
         avatar=self._fileSet('avatar', s, avatar, allowOverwrite=False, strictMode=True)
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
      for l in self.___specialLabel:
         self.labelAdd(userId, l, special=True, strictMode=True)
      self.__authMap[userId]=passwordHash
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
      self.db.set((userId, 'node_date', dateId, 'node_from'), False, strictMode=False, onlyIfExist=False)
      self.db.set((userId, 'node_date', dateId, 'node_to'), False, strictMode=False, onlyIfExist=False)
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

   def labelAdd(self, user, label, descr=None, color=None, special=False, strictMode=False):
      assert label
      if isinstance(label, (str, unicode)):
         if '/' in label:
            label=tuple(s.strip() for s in label.split('/') if s.strip())
         else:
            label=(label,)
      else:
         assert isinstance(label, tuple)
      userId=self.userId(user)
      labelIds=tuple(self.labelId(l) for l in label)
      idsPref=(userId, 'node_label',)
      data={'descr':None, 'color':None, '_special':False}
      for i in xrange(len(labelIds)):
         if i==len(labelIds)-1:
            data={'descr':descr, 'color':color, '_special':special}
         ids=idsPref+labelIds[:i+1]
         data['id']=self.labelIds2human(ids)
         data['name']=label[i]
         data['nameChain']='/'.join(label[:i+1])
         ids=self.db.set(ids, data, strictMode=strictMode, onlyIfExist=False)
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
      #! нужна также проверка, есть ли пользователь в адресатах и доп-хак на случай, если письмо было отправлено самому себе
      userId=self.userId(user)
      return not(self.userSelfEmailCheck(userId, s))

   __map_members=['from', 'to', 'cc', 'bcc', 'reply-to', 'return-path']

   __map_memberRename={
      'reply-to':'replyTo',
      'return-path':'returnPath',
   }

   __map_memberSingle=set((
      'from',
      'replyTo',
      'returnPath'
   ))

   __map_memberLinkMsg={
      'from':'node_from',
      'to':'node_to',
      'cc':'node_to',
      'bcc':'node_to',
   }

   def _msgProc_members(self, user=NULL, dateIds=NULL, data=NULL, linkToMsg=NULL, headers=NULL, **kwargs):
      headers['to']=(headers.get('to') or [])+(headers.get('delivered-to') or [])
      for k in self.__map_members:
         v=headers.get(k) or None
         k=self.__map_memberRename.get(k, k)
         if not v:
            data[k]=None
         else:
            data[k]=v[0][1] if k in self.__map_memberSingle else tuple(_v[1] for _v in v)
         if k not in self.__map_memberLinkMsg: continue
         for vName, vEmail in v or ():
            if not vEmail: continue
            ids=self.emailAdd(user, vEmail, vName, strictMode=False)
            ids=self.db.link(
               dateIds+(self.__map_memberLinkMsg[k], ids[-1]),
               ids, strictMode=False, onlyIfExist=False)
            linkToMsg.append(ids)

   @staticmethod
   def _extract_replyPoint(headers):
      replyTo=headers.get('in-reply-to')
      if not replyTo:
         replyTo=headers.get('references') or ''
         replyTo=tuple(s.strip() for s in replyTo.split(' ') if s.strip())
         if replyTo:
            replyTo=replyTo[-1]
      return replyTo

   def _msgProc_dialog(self, headers=NULL, user=NULL, userId=NULL, dateIds=NULL, linkToMsg=NULL, linkInMsg=NULL, **kwargs):
      replyPoint=self._extract_replyPoint(headers)
      ids=self.dialogFind_byMsg(user, replyPoint, asThread=True) if replyPoint else False
      if ids is False:
         if replyPoint:
            problemIds=self.problemAdd(userId, 'Parent message missed')
            linkInMsg.append((problemIds[-1], problemIds))
         #
         ids=self.dialogAdd(userId)
         ids=self.db.link(
            dateIds+('node_dialog', ids[-1]),
            ids, strictMode=False, onlyIfExist=False)
      linkToMsg.append(ids)

   def _msgProc_attachments(self, msg=NULL, msgNormalized=NULL, attachments=NULL, strictMode=NULL, data=NULL, **kwargs):
      if attachments:
         if not self._supports.get('file'):
            self.workspace.log(2, 'Saving files not supported')
         else:
            for i, o in enumerate(attachments):
               name='%s_%i'%(msgNormalized, i+1)
               content=o.pop('payload')
               if o['binary']:
                  content=base64.b64decode(content)
               else:
                  try:
                     content=content.encode("utf-8")
                  except Exception: pass
               o.pop('binary', None)
               o.pop('content_transfer_encoding', None)
               o['_store_fileId']=self._fileSet('attachment', name, content, nameNormalized=True, allowOverwrite=not(strictMode), strictMode=False)
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
      if isInt(msg): msg=str(msg)
      assert isinstance(msg, (str, unicode))
      assert isinstance(body, tuple) and len(body)==2
      #
      isIncoming=self._msgProc_isIncoming(user, headers, raw, strictMode)
      bodyPlain, bodyHtml=body
      msgNormalized=self._fileNameNormalize(msg)
      rawStored=self._fileSet('raw', msgNormalized+'_raw', raw, nameNormalized=True, allowOverwrite=not(strictMode), strictMode=False)
      data={
         'id':msg,
         'subject':headers['subject'],
         'timestamp':headers['date'],
         'isIncoming':isIncoming,
         '_raw':rawStored,
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

   def dialogFind_byMsgIds(self, ids, asThread=False, strictMode=False):
      g=self.db.iterBacklinks(ids, recursive=False, safeMode=False, calcProperties=False, strictMode=strictMode, allowContextSwitch=False)
      for ids2, _ in g:
         if len(ids2)>4 and ids2[3]=='node_dialog' and ids2[-1]==ids[-1]:
            return ids2 if asThread else ids2[:5]
      raise RuntimeError('Msg founded but no link to dialog')  #! fixme

   def dialogFind_byMsg(self, user, msg, date=None, asThread=False, strictMode=False):
      msgId=self.msgId(msg)
      userId=self.userId(user)
      ids=self.msgFind_byMsg(userId, msgId, date, strictMode=strictMode)
      if ids is None: return False
      return self.dialogFind_byMsgIds(ids, asThread=asThread, strictMode=strictMode)

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
      return self.dialogGet_byIds(ids, returnProps=returnProps, needSortByDate=True)

   def dialogGet_byIds(self, ids, props=None, returnProps=False, needSortByDate=False):
      g=self.db.iterBranch(ids, recursive=True, treeMode=True, safeMode=False, calcProperties=returnProps, skipLinkChecking=True, allowContextSwitch=False)
      if needSortByDate:
         g=sorted(g,
            #! такая сортировка требует обращения к хранилищу данных, которое как правило медленное. при этом типичный сценарий использования - это получение диалога для дальнейшего запроса сообщений из него - что по сути вызовет двойное обращение к хранилищу на каждый обьект.
            key=lambda s: self.db.get(*s, returnRaw=True, strictMode=True)['timestamp'])
      for ids, (props, l) in g:
         yield (ids, props) if returnProps else ids

   def msgGet(self, user, msg, date=None, strictMode=False, onlyPublic=True, resolveAttachments=True, andLabels=True, andDialogId=True):
      ids=self.msgFind_byMsg(user, msg, date, strictMode=strictMode)
      if not ids: return None
      return self.msgGet_byIds(ids, strictMode=strictMode, onlyPublic=onlyPublic, resolveAttachments=resolveAttachments, andLabels=andLabels, andDialogId=andDialogId)

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

   def msgGet_byIds(self, ids, props=None, strictMode=True, onlyPublic=False, resolveAttachments=False, andLabels=False, andDialogId=False, wrapMagic=True):
      if andLabels:
         # получение данных сработает и по ссылке, но для лэйблов нужна прямая адресация
         ids=self.db.resolveLink(ids)
      #
      res=self.db.get(ids, existChecked=props, returnRaw=True, strictMode=strictMode)
      if not strictMode and res is None:
         return {}
      res=self.prepDataForReturn(res, filterPrivateData=onlyPublic, wrapMagic=wrapMagic)
      if resolveAttachments:
         pass  #! fix
      if andDialogId:
         res['dialogId']=self.dialogIds2human(self.dialogFind_byMsgIds(ids, asThread=False, strictMode=True))
      if andLabels:
         #! после VombatiDB#99 можно будет передавать `branch` через аргумент `env` и таким образом кешировать запрос
         res['labels']=tuple(self.db.query(
            branch=ids,
            what='PREP_LABEL(DB.resolveLink(IDS))',
            where='NS=="label"',
            recursive=False,
            env={
               'PREP_LABEL':self.labelIds2human
            }
         ))
      return res

   def userList(self, onlyPublic=True, wrapMagic=True):
      return self.db.query(
         what='INDEX[1:], PREP_DATA(DATA)',
         where='NS=="user"',
         recursive=False,
         env={
            'PREP_DATA':self.JT_prepDataForReturn[(onlyPublic, wrapMagic)]
         }
      )

   def labelList(self, user, countAll=True, countWithLabel=None, byDialog=True, filterPrivateData=True, skipSpecial=True, wrapMagic=True):
      if byDialog:
         raise NotImplementedError
      userId=self.userId(user)
      what=['data={}']
      pre=[]
      if countWithLabel or countAll:
         what.append('mapForCurrent=DB.iterBacklinks(IDS, PROPS, safeMode=False)')
         if countWithLabel:
            if isinstance(countWithLabel, (str, unicode)): countWithLabel=(countWithLabel,)
            what.append('data["countWithLabel"]={}')
            for l in countWithLabel:
               v=re_prepForId.sub('_', l.lower())
               pre.append('mapForLabel_%s=DB.getBacklinks(("%s", "node_label", "%s"), safeMode=False)'%(v, userId, self.labelId(l)))
               what.append('data["countWithLabel"]["%s"]=len(None for ids in mapForCurrent&mapForLabel_%s if ids[1]=="node_date")'%(l, v))
         if countAll:
            what.append('data["countAll"]=len(None for ids in mapForCurrent if ids[1]=="node_date")')
      what.extend((
         'data.update(DATA)',
         'DATA=data',
         'WHAT=(DATA["id"], PREP_DATA(DATA))',
      ))
      return self.db.query(
         pre=pre,
         what=what,
         branch=(userId, 'node_label',),  #! после VombatiDB#99 можно будет передавать `branch` через аргумент `env` и таким образом кешировать запрос
         where='not DATA["_special"]' if skipSpecial else None,
         env={
            'PREP_DATA':self.JT_prepDataForReturn[(filterPrivateData, wrapMagic)],
            'PREP_LABEL':self.labelIds2human,
            'PREP_IDS':self.ids2human,
         }
      )

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
      comment="# OBJ.%s %s '%s'"%(key, match, value)
      if key=='label':
         counter['CHECK_WITH_label']+=1
         var='CHECK_WITH_label%i'%counter['CHECK_WITH_label']
         toPre("%s=set(ids[:-1] for ids, _ in db_iterBacklinks((%r, 'node_label', %r), recursive=False, safeMode=False, calcProperties=False, strictMode=False, allowContextSwitch=False) if ids[1]=='node_date')  %s"%(var, userId, self.labelId(value), comment))
      elif key=='from' or key=='to':
         s='to' if key=='to' else 'from'
         counter['CHECK_WITH_'+s]+=1
         var='CHECK_WITH_%s%i'%(s, counter['CHECK_WITH_'+s])
         toPre("%s=set()  %s"%(var, comment))
         toPre("g=db_iterBacklinks((%r, 'node_email', %r), recursive=False, safeMode=False, calcProperties=False, strictMode=False, allowContextSwitch=False)  %s"%(userId, self.emailId(value), comment))
         toPre("%s.update(*(db_getLinked(ids, strictMode=False, safeMode=False) for ids, _ in g if len(ids)==5 and ids[1]=='node_date' and ids[3]=='node_%s'))  %s"%(var, s, comment))
      else:
         raise ValueError('Unknown key')  #! fix
      toIter("CURR_PART %s= %s  %s"%(self.__finderMatchMap[match], var, comment))

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
            if not o:
               _toIter('pass')
            elif 'key' in o:
               self.__queryCompile_forKey(toPre, _toIter, counter, userId, o['key'], o['value'], o['match'])
            elif len(o)==1:
               self.__queryCompile_forOp(toPre, _toIter, counter, userId, *next(o.iteritems()))
      elif op=='and':
         toIter('# AND <<')
         #
         for o in conds:
            if not o:
               _toIter('pass')
            elif 'key' in o:
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
            if not o:
               _toIter('pass')
            elif 'key' in o:
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
      onIter.append('if CURR_PART: yield date, CURR_PART, gDates')
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
      # code=fileGet('filter_source.py').decode('utf-8')
      fileWrite('filter_source.py', code.encode('utf-8'))
      code+='\nRUN.source="""%s"""'%code
      code=compile(code, self.db.query_envName, 'exec')
      #~ сейчас в code полностью сформированный исходник, однако генератор дат он берет из окружения - значит можно смело кешировать и переиспользовать с другими датами
      return code

   def __queryDateIter(self, dates):
      assert dates
      if isinstance(dates, list): dates=tuple(dates)
      assert dates and isinstance(dates, tuple)
      _ptrnStr=(str, unicode)
      _ptrnDate=datetime.date
      _ptrnDatetime=datetime.datetime
      _fromStr=datetime.datetime.strptime
      _fromInt=datetime.date.fromtimestamp
      _today=datetime.date.today()
      _min=_ptrnDate(1970, 1, 1)  #! нужно получать минимальную дату в базе и использовать это
      _delta=datetime.timedelta
      _yesterday=_today-_delta(days=1)
      _dateId=self.dateId
      _dateFrom=_dateTo=_dateStep=None
      _dateMin=_dateMax=None
      l=len(dates)-1
      for i, val in enumerate(dates):
         if not val:
            raise NotImplementedError  #! fix
         elif isinstance(val, _ptrnStr):
            val=val.lower()
            if val=='today':
               val=_today
            elif val=='yesterday':
               val=_yesterday
            elif val.startswith('today'):
               if '-' in val:
                  ss, m='-', -1
               elif '+' in val:
                  ss, m='+', 1
               else:
                  raise ValueError  #! fix
               val=m*int(val.split(ss, 1)[1])
               val=_today+_delta(days=val)
            else:
               val=_fromStr(val, '%Y%m%d').date()  #? не самый удачный паттерн, не помню откуда он взялся
         elif isInt(val):
            assert val
            assert _dateFrom and _dateStep is None
         elif isinstance(val, _ptrnDate):
            pass
         elif isinstance(val, _ptrnDatetime):
            val=val.date()
         elif val is True:
            assert _dateStep is not None
            val=_today if _dateStep>0 else _min
         else:
            raise IncorrectInputError('Incorrect value for date-iterator: `%r`(%s)'%(val, type(val)))
         #
         if _dateFrom is None: _dateFrom=val
         elif _dateStep is None: _dateStep=val
         elif _dateTo is None: _dateTo=val
         else:
            raise RuntimeError('WTF in date-iterator')
         if _dateFrom is None or _dateStep is None or _dateTo is None:
            if i<l: continue
            raise IncorrectInputError('Missed some values for date-iterator')
         if _dateFrom==_dateTo:
            raise IncorrectInputError('Passed date-from and date-to must not equal')
         if (_dateFrom>_dateTo)==_dateStep>0:
            raise IncorrectInputError('Incorrect date-from and date-to')
         #
         _dateMin=min(_dateMin, _dateFrom, _dateTo) if _dateMin is not None else min(_dateFrom, _dateTo)
         _dateMax=max(_dateMax, _dateFrom, _dateTo) if _dateMax is not None else max(_dateFrom, _dateTo)
         delta=_delta(days=_dateStep)
         d=_dateFrom
         while (d<=_dateTo if _dateStep>0 else d>=_dateTo):
            cmd=yield (d, _dateId(d))
            d+=delta
            while cmd:
               args=()
               if cmd and isinstance(cmd, tuple):
                  cmd, args=cmd[0], cmd[1:]
               if not cmd: break
               elif cmd is self.dialogFindEx.CMD_PACK_DATES:
                  if (d>_dateTo if _dateStep>0 else d<_dateTo):
                     cmd=yield dates[i+1:] or False  # if we out-of-range, slice just returns empty tuple
                  else:
                     cmd=yield (d.strftime('%Y%m%d'), _dateStep, _dateTo.strftime('%Y%m%d'))+dates[i+1:]  # if we out-of-range, slice just returns empty tuple
               elif cmd is self.dialogFindEx.CMD_CHECK_DATE:
                  #! по хорошему здесь нужно быстро обработать весь остаток `dates` для поиска границ
                  _val=args[0]
                  if isInt(_val): _val=_fromInt(_val)
                  elif isinstance(_val, _ptrnDatetime): _val=_val.date()
                  elif isinstance(_val, _ptrnDate): pass
                  else:
                     raise IncorrectInputError('Incorrect date value `%s`: %s'%(_val, type(_val)))
                  cmd=yield (_val>=_dateMin and _val<=_dateMax)
               else:
                  raise IncorrectInputError('Incorrect command')
         _dateFrom=_dateTo=_dateStep=None

   def dialogFindEx(self, user, query, dates):
      dateIterator=self.__queryDateIter(dates)
      userId=self.userId(user)
      q=self.__queryCompile(userId, query)
      return self.db.query(q=q, env={'DATES':dateIterator})

   dialogFindEx.CMD_PACK_DATES=object()
   dialogFindEx.CMD_CHECK_DATE=object()
