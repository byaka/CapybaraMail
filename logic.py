# -*- coding: utf-8 -*-
from functionsex import *
from VombatiDB import VombatiDB, showDB, showStats, WorkspaceOld
from VombatiDB import errors as dbError

from scheme import SCHEME
from errors import CapybaraMailError, AccessDeniedError

class Logic(object):
   def __init__(self, workspace, reinitNamespaces=True):
      self.__hook={
         'ticket':{},
      }
      self.workspace=workspace
      if 'dbPath' not in self.workspace:
         self.workspace.dbPath=getScriptPath(real=True, f=__file__)+'/db'
      _workspace=WorkspaceOld(self.workspace)
      self.db=VombatiDB(('NS', 'Columns', 'MatchableLinks', 'StorePersistentWithCache', 'Search'))(_workspace, self.workspace.dbPath)
      self._configureDB(reinitNamespaces=reinitNamespaces)

   def _configureDB(self, reinitNamespaces):
      self.db.settings.store_flushOnChange=False
      self.db.settings.ns_checkIndexOnConnect=True  #? было `not reinitNamespaces`
      self.db.settings.dataMerge_ex=True
      self.db.settings.dataMerge_deep=False
      self.db.settings.linkedChilds_default_do=False
      self.db.settings.linkedChilds_inheritNSFlags=False
      self.db.settings.ns_default_allowLocalAutoIncrement=False
      self.db.settings.columns_default_allowUnknown=False
      self.db.settings.columns_default_allowMissed=False
      self.db.connect()
      if reinitNamespaces:
         self.db.configureNS(SCHEME, andClear=True)
      self._initProjectMap()

   def _initProjectMap(self):
      self.__projectMap=defaultdict(dict)
      g=self.db.query(what='NS, IDS[0], (INDEX or "")[1:], DATA')
      for ns, userId, project, o in g:
         if ns=='project':
            type=o["type"]
            pId2=PROJECT_TYPE[type][3](o["externalId"])
            self.__projectMap[type][pId2]=(userId, project, o.isActive)
         if ns!='user':
            g.send(False)

   @staticmethod
   def tokenId(token):
      if token.startswith('token#'): return token
      return 'token#%s'%token

   @staticmethod
   def userId(email):
      if email.startswith('user#'): return email
      return 'user#%s'%email

   @staticmethod
   def managerId(email):
      if email.startswith('manager#'): return email
      return 'manager#%s'%email

   @staticmethod
   def projectId(project):
      if project.startswith('project#'): return project
      return 'project#%s'%project

   @staticmethod
   def sharedProjectId(project):
      if project.startswith('sharedProject#'): return project
      return 'sharedProject#%s'%project

   @staticmethod
   def operatorId(login):
      if login.startswith('operator#'): return login
      loginHash=sha1(login)  #? sha1 ненадежен, а остальные слишком длинные, неудобно дебажить
      return 'operator#%s'%loginHash

   def tokenAdd(self, type, ids, password):
      #? при каждой авторизации для тогоже пользователя создается новый токен. это хороший подход с точки зрение безопасности, но как быть со старыми токенами - они забьют всю базу
      assert isStr(password) and password
      assert isStr(type) and type
      data={}
      idStr=''
      if type=='user':
         assert isStr(ids) and ids
         userId=self.userId(ids)
         try:
            o=self.db.get(userId, returnRaw=True, strictMode=True)
         except dbError.NotExistError:
            raise LogicError(-104)
         data['userId']=userId
         idStr=userId
      elif type=='operator':
         assert ids and isinstance(ids, tuple) and len(ids)==2
         userId, login=ids
         assert isStr(userId) and userId
         assert isStr(login) and login
         userId=self.userId(userId)
         try:
            print '!!!', (userId, self.operatorId(login))
            o=self.db.get((userId, self.operatorId(login)), returnRaw=True, strictMode=True)
         except dbError.NotExistError:
            raise LogicError(-304)
         data['userIdOfOperator']=userId
         data['login']=login
         idStr=userId+'|'+login
      elif type=='manager':
         assert ids and isinstance(ids, tuple) and len(ids)==2
         userId, manager=ids
         assert isStr(userId) and userId
         assert isStr(manager) and manager
         userId=self.userId(userId)
         try:
            o=self.db.get((userId, self.managerId(manager)), returnRaw=True, strictMode=True)
         except dbError.NotExistError:
            raise LogicError(-504)
         data['userIdOfManager']=userId
         data['manager']=manager
         idStr=userId+'|'+manager
      else:
         raise TypeError('Unknown token type')
      #
      passwordHash=password  #! здесь добавить хеширование
      if o['passwordHash']!=passwordHash or not o['isActive']:
         raise AccessDeniedError()
      # generating token
      token=(str(time.time()), type, idStr, str(random.random()), str(time.time()))
      token=sha512('--'.join(token))
      data={
         'type':type,
         'ids':data,
         'created':datetime_now(),
         'lastSeen':None,
      }
      self.db.set(self.tokenId(token), data, strictMode=True, onlyIfExist=False)
      return token

   def tokenCheck(self, token):
      assert isStr(token) and token
      tId=self.tokenId(token)
      try:
         o=self.db.get(tId, returnRaw=True, strictMode=True)
      except dbError.NotExistError:
         raise AccessDeniedError()
      self.db.set(tId, {'lastSeen':datetime_now()}, allowMerge=True, strictMode=True, onlyIfExist=True)
      return o['ids']

   def userAdd(self, email, password, info):
      assert isStr(email) and email
      assert regExp_isEmail.match(email)
      assert isStr(password) and password
      assert isinstance(info, (dict, types.NoneType))
      if self.db.isExist(self.managerId(email)):
         raise LogicError(-106)
      userId=self.userId(email)
      passwordHash=password  #! здесь добавить хеширование
      data={'passwordHash':passwordHash, 'isActive':True, 'info':info}
      try:
         self.db.set(userId, data, strictMode=True, onlyIfExist=False)
      except dbError.ExistStatusMismatchError:
         raise LogicError(-101)
      return userId

   def userEdit(self, info, userId):
      assert isinstance(info, (dict, types.NoneType))
      assert isStr(userId) and userId
      #! пока `dataMerge_deep` отключен, при обновлении `info` необходимо передавать полную копию
      try:
         self.db.set(userId, {'info':info}, allowMerge=True, strictMode=True, onlyIfExist=True)
      except dbError.ExistStatusMismatchError:
         raise LogicError(-104)

   def userChangePassword(self, passwordNew, passwordOld, userId):
      assert isStr(passwordOld) and passwordOld
      assert isStr(passwordNew) and passwordNew
      assert isStr(userId) and userId
      o=self.db.get(userId, returnRaw=True, strictMode=True)
      passwordHash=passwordOld  #! здесь добавить хеширование
      if o['passwordHash']!=passwordHash:
         raise AccessDeniedError()
      passwordHash=passwordNew  #! здесь добавить хеширование
      try:
         self.db.set(userId, {'passwordHash':passwordHash}, allowMerge=True, strictMode=True, onlyIfExist=True)
      except dbError.ExistStatusMismatchError:
         raise LogicError(-104)

   def userInfo(self, userId, extended=True):
      assert isStr(userId) and userId
      try:
         r=self.db.get((userId,), returnRaw=True, strictMode=True)
      except dbError.NotExistError:
         raise LogicError(-104)
      r=r.copy()
      for k in QUERY_TEMPLATE['safeUser_raw']:
         if k in r: del r[k]
      r['email']=userId.split('user#', 1)[-1]
      if extended:
         projectCount=sum(self.db.query(what="1", branch=(userId,), where="NS==\"project\"", recursive=False, allowCache=True))
         operatorCount=sum(self.db.query(what="1", branch=(userId,), where="NS==\"operator\"", recursive=False, allowCache=True))
         r['projects']=projectCount
         r['operators']=operatorCount
      return r

   def managerAdd(self, userId, email, password, info, rights):
      assert isStr(userId) and userId
      assert isStr(email) and email
      assert regExp_isEmail.match(email)
      assert isStr(password) and password
      assert isinstance(info, (dict, types.NoneType))
      assert isinstance(rights, (dict, types.NoneType))
      if self.db.isExist(self.userId(email)):
         raise LogicError(-506)
      mId=self.managerId(email)
      passwordHash=password  #! здесь добавить хеширование
      data={'passwordHash':passwordHash, 'isActive':True, 'info':info}
      for n in ('allowOperatorAdd', 'allowOperatorEdit', 'allowOperatorChangeStatus', 'allowOperatorChangePassword', 'allowManagerAdd', 'allowManagerEdit', 'allowManagerChangeStatus', 'allowManagerChangePassword'):
         v=False if rights is None else rights.get(n, False)
         assert v is True or v is False, 'Rights must be bools'
         data[n]=v
      try:
         self.db.set((userId, mId), data, strictMode=True, onlyIfExist=False)
      except dbError.ExistStatusMismatchError:
         raise LogicError(-501)
      return email

   def managerCheckSelf(self, userId, manager):
      mId=self.managerId(manager)
      o=self.db.get((userId, mId), returnRaw=True, strictMode=True)
      assert o['isActive']
      return o.copy()

   def managerCheckProject(self, userId, manager, project):
      mId=self.managerId(manager)
      pId=self.projectId(project)
      assert self.db.get((userId, pId), returnRaw=True, strictMode=True)['isActive']
      shpId=self.sharedProjectId(project)
      o=self.db.get((userId, mId, shpId), returnRaw=True, strictMode=True)
      return o.copy()

   def managerChangePassword(self, userId, manager, passwordNew, passwordOld):
      assert isStr(userId) and userId
      assert isStr(manager) and manager
      assert isStr(passwordNew) and passwordNew
      mId=self.managerId(manager)
      if passwordOld is not NULL:
         # админ может менять пароли менеджеров, незная их
         #~ но в отличии от аналогичного метода у операторов, решение о том что это админ принимается на стороне ресолвера
         assert isStr(passwordOld) and passwordOld
         o=self.db.get((userId, mId), returnRaw=True, strictMode=True)
         passwordHash=passwordOld  #! здесь добавить хеширование
         if o['passwordHash']!=passwordHash:
            raise AccessDeniedError()
      passwordHash=passwordNew  #! здесь добавить хеширование
      try:
         self.db.set((userId, mId), {'passwordHash':passwordHash}, allowMerge=True, strictMode=True, onlyIfExist=True)
      except dbError.ExistStatusMismatchError:
         raise LogicError(-504)

   def managerEdit(self, userId, manager, info=None, rights=None):
      assert isStr(userId) and userId
      assert isStr(manager) and manager
      assert isinstance(info, (dict, types.NoneType))
      assert isinstance(rights, (dict, types.NoneType))
      assert info is not None or rights is not None
      mId=self.managerId(manager)
      data={}
      if rights is not None:
         for n in ('allowOperatorAdd', 'allowOperatorEdit', 'allowOperatorChangeStatus', 'allowOperatorChangePassword', 'allowManagerAdd', 'allowManagerEdit', 'allowManagerChangeStatus', 'allowManagerChangePassword'):
            v=rights.get(n, False)
            assert v is True or v is False, 'Rights must be bools'
            data[n]=v
      if info is not None:
         data['info']=info
      #! пока `dataMerge_deep` отключен, при обновлении `info` необходимо передавать полную копию
      try:
         self.db.set((userId, mId), data, allowMerge=True, strictMode=True, onlyIfExist=True)
      except dbError.ExistStatusMismatchError:
         raise LogicError(-504)

   def managerChangeStatus(self, userId, manager, active):
      assert isBool(active)
      assert isStr(userId) and userId
      assert isStr(manager) and manager
      mId=self.managerId(manager)
      try:
         self.db.set((userId, mId), {'isActive':active}, allowMerge=True, strictMode=True, onlyIfExist=True)
      except dbError.ExistStatusMismatchError:
         raise LogicError(-504)

   def managerList(self, userId, fullInfo=True, onlyProjects=None):
      assert isStr(userId) and userId
      assert isinstance(fullInfo, bool)
      assert isinstance(onlyProjects, (list, set, tuple, types.NoneType))
      if not self.db.isExist(userId):
         raise LogicError(-104)
      if fullInfo:
         q='(%(idManager)s, dict(((k,DATA[k]) for k in DATA if %(safeManager)s), projects=sum(DB.query(what="1", branch=IDS, where="NS==\\\"sharedProject\\\"", recursive=False, allowCache=True))))'  # noqa: E501
         q=q%QUERY_TEMPLATE
      else:
         q=QUERY_TEMPLATE['idManager']
      where='NS=="manager"'
      if onlyProjects:
         where+=' and (%s)'%(' or '.join('DB.isExist(IDS+("%s",))'%self.sharedProjectId(s) for s in onlyProjects))
      return self.db.query(what=q, branch=userId, where=where, recursive=False)

   def managerInfo(self, userId, manager, extended=True):
      assert isStr(manager) and manager
      assert isStr(userId) and userId
      mId=self.managerId(manager)
      try:
         r=self.db.get((userId, mId), returnRaw=True, strictMode=True)
      except dbError.NotExistError:
         raise LogicError(-504)
      r=r.copy()
      r['email']=manager
      for k in QUERY_TEMPLATE['safeManager_raw']:
         if k in r: del r[k]
      if extended:
         projectCount=sum(self.db.query(what="1", branch=(userId, mId), where="NS==\"sharedProject\"", recursive=False, allowCache=True))
         r['projects']=projectCount
      return r

   def managerAccess(self, userId, project, manager, grant, rights=None):
      assert isBool(grant)
      assert isStr(userId) and userId
      assert isStr(project) and project
      assert isStr(manager) and manager
      mId=self.managerId(manager)
      if not self.db.isExist((userId, mId)):
         raise LogicError(-504)
      pId=self.projectId(project)
      if not self.db.isExist((userId, pId)):
         raise LogicError(-204)
      shpId=self.sharedProjectId(project)
      if grant:
         assert isinstance(rights, (dict, types.NoneType))
         # Grant access
         data={}
         for n in ('allowChangeStatus', 'allowEdit', 'allowOperatorAssign', 'allowManagerAssign'):
            v=False if rights is None else rights.get(n, False)
            assert v is True or v is False, 'Rights must be bools'
            data[n]=v
         self.db.set((userId, mId, shpId), data, allowMerge=True, strictMode=False)
      else:
         # Revoke access
         self.db.remove((userId, mId, shpId), strictMode=False)

   def managerHaveAccess(self, userId, manager, fullInfo=True):
      assert isStr(userId) and userId
      assert isBool(fullInfo)
      assert isStr(manager) and manager
      mId=self.managerId(manager)
      if not self.db.isExist((userId, mId)):
         raise LogicError(-504)
      if fullInfo:
         q='(%(idProject3)s, dict(DATA.items()+[(k,v) for k,v in DB.get(("'+userId+'", "project#"+%(idProject3)s), returnRaw=True, strictMode=True).iteritems() if %(safeProject)s], operators=sum(DB.query(what="1", branch=("'+userId+'", "project#"+%(idProject3)s), where="NS==\\\"operator\\\" and DATA[\\\"isActive\\\"]", recursive=False, allowCache=True))))'
         q=q%QUERY_TEMPLATE
      else:
         q=QUERY_TEMPLATE['idManager']
      return self.db.query(what=q, branch=(userId, mId), where='NS=="sharedProject"', recursive=False)

   def projectResolve(self, type, externalId, alreadyConverted=False):
      assert isStr(type) and type and type in PROJECT_TYPE
      assert externalId
      if type not in self.__projectMap:
         raise LogicError(-204)
      pId2=externalId if alreadyConverted else PROJECT_TYPE[type][3](externalId)
      if pId2 not in self.__projectMap[type]:
         raise LogicError(-204)
      return self.__projectMap[type][pId2]

   def projectAdd(self, userId, name, type, externalId, info):
      assert isStr(userId) and userId
      assert isStr(name) and name
      assert isinstance(info, (dict, types.NoneType))
      assert isStr(type) and type and type in PROJECT_TYPE
      assert externalId
      assert PROJECT_TYPE[type][0](externalId)
      internalId=PROJECT_TYPE[type][1](externalId)
      project=PROJECT_TYPE[type][2](internalId)
      pId=self.projectId(project)
      data={'name':name, 'type':type, 'externalId':externalId, 'internalId':internalId, 'isActive':True, 'info':info}
      try:
         self.db.set((userId, pId), data, strictMode=True, onlyIfExist=False)
      except dbError.ExistStatusMismatchError:
         raise LogicError(-201)
      pId2=PROJECT_TYPE[type][3](externalId)
      self.__projectMap[type][pId2]=(userId, project, data['isActive'])
      return project

   def projectAccessibleBy(self, userId, project, fullInfo=True):
      assert isBool(fullInfo)
      assert isStr(userId) and userId
      assert isStr(project) and project
      pId=self.projectId(project)
      if not self.db.isExist((userId, pId)):
         raise LogicError(-204)
      if fullInfo:
         # q='(%(idOperator)s, { k:DATA[k] for k in DATA if %(safeOperator)s})'
         q='(%(idOperator)s, dict(((k,DATA[k]) for k in DATA if %(safeOperator)s), projects=sum(DB.query(what="1", branch=("'+userId+'", IDS[-1]), where="NS==\\\"project\\\" and DATA[\\\"isActive\\\"]", recursive=False, allowCache=True))))'  # noqa: E501
         q=q%QUERY_TEMPLATE
      else:
         q=QUERY_TEMPLATE['idOperator']
      return self.db.query(what=q, branch=(userId, pId), where='NS=="operator"', recursive=False)

   def projectEdit(self, userId, project, name=NULL, info=NULL):
      if name is NULL and info is NULL: return
      assert isStr(project) and project
      assert isStr(userId) and userId
      if info is not NULL:
         assert isinstance(info, (dict, types.NoneType))
      if name is not NULL:
         assert isStr(name) and name
      pId=self.projectId(project)
      data={}
      if info is not NULL: data['info']=info
      if name is not NULL: data['name']=name
      #! пока `dataMerge_deep` отключен, при обновлении `info` необходимо передавать полную копию
      try:
         self.db.set((userId, pId), data, allowMerge=True, strictMode=True, onlyIfExist=True)
      except dbError.ExistStatusMismatchError:
         raise LogicError(-204)

   def projectList(self, userId, fullInfo=True):
      assert isStr(userId) and userId
      assert isinstance(fullInfo, bool)
      if not self.db.isExist(userId):
         raise LogicError(-104)
      if fullInfo:
         q='(%(idProject)s, dict(((k,DATA[k]) for k in DATA if %(safeProject)s), operators=sum(DB.query(what="1", branch=IDS, where="NS==\\\"operator\\\" and DATA[\\\"isActive\\\"]", recursive=False, allowCache=True))))'  # noqa: E501
         q=q%QUERY_TEMPLATE
      else:
         q=QUERY_TEMPLATE['idProject']
      return self.db.query(what=q, branch=userId, where='NS=="project"', recursive=False)

   def projectChangeStatus(self, userId, project, active):
      assert isBool(active)
      assert isStr(userId) and userId
      assert isStr(project) and project
      pId=self.projectId(project)
      try:
         self.db.set((userId, pId), {'isActive':active}, allowMerge=True, strictMode=True, onlyIfExist=True)
         o=self.db.get((userId, pId), returnRaw=True, strictMode=True)
         type, externalId=o['type'], o['externalId']
         pId2=PROJECT_TYPE[type][3](externalId)
         self.__projectMap[type][pId2]=(userId, project, active)
      except dbError.ExistStatusMismatchError:
         raise LogicError(-204)

   def operatorAdd(self, userId, login, password, info):
      assert isStr(login) and login
      assert isStr(userId) and userId
      assert isStr(password) and password
      assert isinstance(info, (dict, types.NoneType))
      oId=self.operatorId(login)
      passwordHash=password  #! здесь добавить хеширование
      data={'passwordHash':passwordHash, 'isActive':True, 'lastSeen':None, 'login':login, 'info':info}
      try:
         self.db.set((userId, oId), data, strictMode=True, onlyIfExist=False)
      except dbError.ExistStatusMismatchError:
         raise LogicError(-301)
      return login

   def operatorChangePassword(self, login, passwordNew, passwordOld=None, userId=NULL, userIdOfOperator=NULL):
      assert isStr(login) and login
      assert isStr(passwordNew) and passwordNew
      if userIdOfOperator is not NULL: userId=userIdOfOperator
      assert isStr(userId) and userId
      oId=self.operatorId(login)
      if userIdOfOperator is not NULL:
         # админ может менять пароли операторов, незная их
         assert isStr(passwordOld) and passwordOld
         o=self.db.get((userId, oId), returnRaw=True, strictMode=True)
         passwordHash=passwordOld  #! здесь добавить хеширование
         if o['passwordHash']!=passwordHash:
            raise AccessDeniedError()
      passwordHash=passwordNew  #! здесь добавить хеширование
      try:
         self.db.set((userId, oId), {'passwordHash':passwordHash}, allowMerge=True, strictMode=True, onlyIfExist=True)
      except dbError.ExistStatusMismatchError:
         raise LogicError(-304)

   def operatorEdit(self, login, info, userId=NULL, userIdOfOperator=NULL):
      assert isStr(login) and login
      assert isinstance(info, (dict, types.NoneType))
      if userIdOfOperator is not NULL: userId=userIdOfOperator
      assert isStr(userId) and userId
      oId=self.operatorId(login)
      #! пока `dataMerge_deep` отключен, при обновлении `info` необходимо передавать полную копию
      try:
         self.db.set((userId, oId), {'info':info}, allowMerge=True, strictMode=True, onlyIfExist=True)
      except dbError.ExistStatusMismatchError:
         raise LogicError(-304)

   def operatorList(self, userId, fullInfo=True):
      assert isStr(userId) and userId
      assert isinstance(fullInfo, bool)
      if not self.db.isExist(userId):
         raise LogicError(-104)
      if fullInfo:
         q='(%(idOperator)s, dict(((k,DATA[k]) for k in DATA if %(safeOperator)s), projects=sum(DB.query(what="1", branch=IDS, where="NS==\\\"project\\\" and DATA[\\\"isActive\\\"]", recursive=False, allowCache=True))))'  # noqa: E501
         q=q%QUERY_TEMPLATE
      else:
         q=QUERY_TEMPLATE['idOperator']
      return self.db.query(what=q, branch=userId, where='NS=="operator"', recursive=False)

   def operatorInfo(self, login, userId=NULL, userIdOfOperator=NULL, extended=True):
      assert isStr(login) and login
      if userIdOfOperator is not NULL: userId=userIdOfOperator
      assert isStr(userId) and userId
      oId=self.operatorId(login)
      try:
         r=self.db.get((userId, oId), returnRaw=True, strictMode=True)
      except dbError.NotExistError:
         raise LogicError(-304)
      r=r.copy()
      for k in QUERY_TEMPLATE['safeOperator_raw']:
         if k in r: del r[k]
      if extended:
         projectCount=sum(self.db.query(what="1", branch=(userId, oId), where="NS==\"project\"", recursive=False, allowCache=True))  #! при листинге считаются только активные, а здесь все
         r['projects']=projectCount
      return r

   def operatorAccess(self, userId, project, login, grant):
      assert isBool(grant)
      assert isStr(userId) and userId
      assert isStr(project) and project
      assert isStr(login) and login
      oId=self.operatorId(login)
      if not self.db.isExist((userId, oId)):
         raise LogicError(-304)
      pId=self.projectId(project)
      if not self.db.isExist((userId, pId)):
         raise LogicError(-204)
      if grant:
         # Grant access
         self.db.link((userId, pId, oId), (userId, oId), strictMode=False, onlyIfExist=False)
         self.db.link((userId, oId, pId), (userId, pId), strictMode=False, onlyIfExist=False)
      else:
         # Revoke access
         self.db.remove((userId, pId, oId), strictMode=False)
         self.db.remove((userId, oId, pId), strictMode=False)

   def operatorHaveAccess(self, login, userId=NULL, userIdOfOperator=NULL, fullInfo=True):
      assert isBool(fullInfo)
      assert isStr(login) and login
      if userIdOfOperator is not NULL: userId=userIdOfOperator
      assert isStr(userId) and userId
      oId=self.operatorId(login)
      if not self.db.isExist((userId, oId)):
         raise LogicError(-304)
      if fullInfo:
         # q='(%(idProject)s, { k:DATA[k] for k in DATA if %(safeProject)s})'
         q='(%(idProject)s, dict(((k,DATA[k]) for k in DATA if %(safeProject)s), operators=sum(DB.query(what="1", branch=("'+userId+'", IDS[-1]), where="NS==\\\"operator\\\" and DATA[\\\"isActive\\\"]", recursive=False, allowCache=True))))'  # noqa: E501
         q=q%QUERY_TEMPLATE
      else:
         q=QUERY_TEMPLATE['idProject']
      return self.db.query(what=q, branch=(userId, oId), where='NS=="project"', recursive=False)

   def operatorChangeStatus(self, userId, login, active):
      assert isBool(active)
      assert isStr(userId) and userId
      assert isStr(login) and login
      oId=self.operatorId(login)
      try:
         self.db.set((userId, oId), {'isActive':active}, allowMerge=True, strictMode=True, onlyIfExist=True)
      except dbError.ExistStatusMismatchError:
         raise LogicError(-304)

   @staticmethod
   def ticketId(ticket):
      if ticket.startswith('ticket#'): return ticket
      return 'ticket#%s'%ticket

   def externalId2ticket(self, type, externalId):
      if type not in TICKET_TYPE:
         raise LogicError(0, 'Unknown ticket type')
      assert TICKET_TYPE[type][0](externalId)
      internalId=TICKET_TYPE[type][1](externalId)
      ticket=TICKET_TYPE[type][2](internalId)
      return internalId, ticket

   def ticketOpen(self, userId, project, type, externalId, tags=None, comment=None):
      assert isStr(userId) and userId
      assert isStr(project) and project
      assert tags is None or isinstance(tags, set)
      assert comment is None or isStr(comment)
      assert isStr(type) and type and type in TICKET_TYPE
      assert externalId
      internalId, ticket=self.externalId2ticket(type, externalId)
      tId=self.ticketId(ticket)
      userId=self.userId(userId)
      pId=self.projectId(project)
      o=self.db.get((userId, pId), returnRaw=True)
      if not o:
         raise LogicError(-204)
      if not o.get('isActive'):
         raise LogicError(-205)
      #? изза того, что эта проверка дергает ветку `ticketsClosed`, это очень неоптимальное решение. возможно это какраз тот случай, когда вместо `internalId` нужно использовать `NS#autogen` для генерации уникального айди вне зависимости от ветви. однако этот автоинкремент считается для всей базы целеком, без разбивки на пользователей или проекты. как вариант - считать автоинкремент вручную для каждого проекта и хранить его внутри проекта. поскольку предполагается, что проекты всегда находятся в памяти - это не должно давать лишнего оверхеда
      for status in ('ticketsOpen', 'ticketsTaken', 'ticketsClosed'):
         if self.db.isExist((userId, pId, status, tId)):
            raise LogicError(-401)
      self.db.set((userId, pId, 'ticketsOpen'), True, strictMode=False, onlyIfExist=False)
      data={
         'type':type,
         'externalId':externalId,
         'internalId':internalId,
         'openTime':datetime_now(),
         'takenTime':None,
         'closedTime':None,
         'tag':tags,
         'comment':comment,
         'operators':None,
      }
      try:
         self.db.set((userId, pId, 'ticketsOpen', tId), data, strictMode=True, onlyIfExist=False)
      except dbError.ExistStatusMismatchError:
         raise LogicError(-401)
      self.__ticketChanged(userId, project, ticket, data=data, changed='all', pId=pId, tId=tId)
      return ticket

   def ticketEdit(self, project, ticket, userId=NULL, login=NULL, userIdOfOperator=NULL, tags=NULL, comment=NULL, andClose=False):
      if userIdOfOperator is not NULL: userId=userIdOfOperator
      if tags is NULL and comment is NULL and not andClose: return
      if tags is not NULL:
         assert tags is None or isinstance(tags, set)
      if comment is not NULL:
         assert comment is None or isStr(comment)
      assert isStr(userId) and userId
      assert isStr(project) and project
      assert isStr(ticket) and ticket
      pId=self.projectId(project)
      tId=self.ticketId(ticket)
      if login is not NULL:
         assert isStr(login) and login
         oId=self.operatorId(login)
         if not self.db.isExist((userId, oId, pId, tId)):
            raise LogicError(-3)
         try:
            o=self.db.get((userId, pId, oId), returnRaw=True, strictMode=True)
         except dbError.ParentNotExistError:
            raise LogicError(-204)
         if o is None:
            raise LogicError(-304)
         if not o['isActive']:
            raise LogicError(-305)
      if not self.db.get((userId, pId), returnRaw=True).get('isActive'):
         raise LogicError(-205)
      status, tData=None, None
      for status in ('ticketsOpen', 'ticketsTaken', 'ticketsClosed'):
         tData=self.db.get((userId, pId, status, tId), returnRaw=True)
         if tData is not None: break
      else:
         raise LogicError(-404)
      data={}
      if tags is not NULL:
         data['tag']=tags
      if comment is not NULL:
         data['comment']=comment
      if andClose:
         # unlinking
         if tData['operators']:
            for s in tData['operators']:
               oId2=self.operatorId(s)
               self.db.remove((userId, oId2, pId, tId), strictMode=False)
               self.db.remove((userId, pId, oId2, tId), strictMode=False)
         # moving ticket to another status-branch
         if status!='ticketsClosed':
            self.db.set((userId, pId, 'ticketsClosed'), True, strictMode=False, onlyIfExist=False)
            self.db.move(
               (userId, pId, status, tId),
               (userId, pId, 'ticketsClosed', tId),
               onlyIfExist=False, strictMode=True, fixLinks=True)
            status='ticketsClosed'
            data['closedTime']=datetime_now()
      if data:
         self.db.set((userId, pId, status, tId), data, allowMerge=True, strictMode=True, onlyIfExist=True)
         self.__ticketChanged(userId, project, ticket, data=(userId, pId, status, tId), changed=','.join(data), pId=pId, tId=tId)

   def ticketTake(self, project, ticket, login, userId=NULL, userIdOfOperator=NULL):
      if userIdOfOperator is not NULL: userId=userIdOfOperator
      assert isStr(userId) and userId
      assert isStr(project) and project
      assert isStr(ticket) and ticket
      assert isStr(login) and login
      pId=self.projectId(project)
      oId=self.operatorId(login)
      if not self.db.isExist((userId, pId, oId)):
         raise LogicError(-3)
      if not self.db.get((userId, pId), returnRaw=True).get('isActive'):
         raise LogicError(-205)
      o=self.db.get((userId, oId), returnRaw=True)
      if o is None:
         raise LogicError(-304)
      if not o['isActive']:
         raise LogicError(-305)
      tId=self.ticketId(ticket)
      status, tData=None, None
      for status in ('ticketsOpen', 'ticketsTaken'):
         tData=self.db.get((userId, pId, status, tId), returnRaw=True)
         if tData is not None: break
      else:
         raise LogicError(-404)
      data={}
      # moving ticket to another status-branch
      if status!='ticketsTaken':
         self.db.set((userId, pId, 'ticketsTaken'), True, strictMode=False, onlyIfExist=False)
         self.db.move(
            (userId, pId, status, tId),
            (userId, pId, 'ticketsTaken', tId),
            onlyIfExist=False, strictMode=True, fixLinks=True)
         status='ticketsTaken'
         data['takenTime']=datetime_now()
      # add operator's login to ticket's column `operators`
      if tData['operators'] is None:
         data['operators']=set((login,))
      elif login not in tData['operators']:
         data['operators']=tData['operators'].copy()
         data['operators'].add(login)
      if data:
         self.db.set((userId, pId, status, tId), data, allowMerge=True, strictMode=True, onlyIfExist=True)
         self.__ticketChanged(userId, project, ticket, data=(userId, pId, status, tId), changed=','.join(data), pId=pId, tId=tId)
      # linking
      self.db.link(
         (userId, oId, pId, tId),
         (userId, pId, status, tId),
         strictMode=True, onlyIfExist=False)
      self.db.link(
         (userId, pId, oId, tId),
         (userId, pId, status, tId),
         strictMode=True, onlyIfExist=False)
      #? по изначальной задумке внутри тикета также создается ссылка на оператора, но в таком случае при переносе тикета все эти ссылки придется также переносить - а значит выполнить итерацию по всей ветке тикета. при этом вродебы единственное применение этой ссылке - узнать подключенных к тикету операторов, но эту информацию можно получить из поля `operators` пусть и придется дернуть store. предполагалось, что благодаря более быстрому доступу через индекс (без дерганья стора) такой подход позволит получить текущих операторов в realtime-api.
      #! если ссылки на операторов внутри тикета не нужны - подправить схему базы

   def ticketUntake(self, userId, project, ticket, login):
      assert isStr(userId) and userId
      assert isStr(project) and project
      assert isStr(ticket) and ticket
      assert isStr(login) and login
      pId=self.projectId(project)
      if not self.db.get((userId, pId), returnRaw=True).get('isActive'):
         raise LogicError(-205)
      oId=self.operatorId(login)
      if not self.db.isExist((userId, pId, oId)):
         raise LogicError(-304)
      tId=self.ticketId(ticket)
      if not self.db.isExist((userId, pId, oId, tId)): return
      status, tData=None, None
      for status in ('ticketsOpen', 'ticketsTaken', 'ticketsClosed'):
         tData=self.db.get((userId, pId, status, tId), returnRaw=True)
         if tData is not None: break
      else:
         raise LogicError(-404)
      data={}
      # remove operator's login to ticket's column `operators`
      if tData['operators'] is not None and login in tData['operators']:
         if len(tData['operators'])==1:
            data['operators']=None
         else:
            data['operators']=tData['operators'].copy()
            data['operators'].remove(login)
      # unlinking
      self.db.remove((userId, oId, pId, tId), strictMode=False)
      self.db.remove((userId, pId, oId, tId), strictMode=False)
      # moving ticket to another status-branch
      if data.get('operators', False) is None and status=='ticketsTaken':  # nothing to do if it closed or open
         self.db.set((userId, pId, 'ticketsOpen'), True, strictMode=False, onlyIfExist=False)
         self.db.move(
            (userId, pId, status, tId),
            (userId, pId, 'ticketsOpen', tId),
            onlyIfExist=False, strictMode=True, fixLinks=True)
         status='ticketsOpen'
         data['takenTime']=None
      if data:
         self.db.set((userId, pId, status, tId), data, allowMerge=True, strictMode=True, onlyIfExist=True)
         self.__ticketChanged(userId, project, ticket, data=(userId, pId, status, tId), changed=','.join(data), pId=pId, tId=tId)

   def ticketList(self, project=None, login=NULL, userId=NULL, userIdOfOperator=NULL, condition=None, fromOpen=True, fromTaken=True, fromClosed=False, fullInfo=True):
      if userIdOfOperator is not NULL: userId=userIdOfOperator
      assert isStr(userId) and userId
      if project is not None:
         if not project: return ()
         assert isinstance(project, (str, unicode, list, tuple))
         if isStr(project): project=(project,)
      assert isinstance(condition, (str, unicode, types.NoneType))
      assert isinstance(fullInfo, bool)
      if not fromOpen and not fromTaken and not fromClosed: return ()
      if login is not NULL:
         assert isStr(login) and login
         oId=self.operatorId(login)
         o=self.db.get((userId, oId), returnRaw=True)
         if o is None:
            raise LogicError(-304)
         if not o['isActive']:
            raise LogicError(-305)
         if project:
            tArr=[]
            for pId in project:
               pId=self.projectId(pId)
               if not self.db.isExist((userId, pId, oId)):
                  raise LogicError(-3)
               if not self.db.get((userId, pId), returnRaw=True).get('isActive'):
                  raise LogicError(-205)
               tArr.append(pId)
            project=tArr
         else:
            project=list(self.db.query(what='IDS[-1]', branch=(userId, oId), where='NS=="project" and DATA["isActive"]', recursive=False))
      elif project:
            tArr=[]
            for pId in project:
               pId=self.projectId(pId)
               if not self.db.get((userId, pId), returnRaw=True).get('isActive'):
                  raise LogicError(-205)
               tArr.append(pId)
            project=tArr
      else:
         project=list(self.db.query(what='IDS[-1]', branch=(userId,), where='NS=="project" and DATA["isActive"]', recursive=False))
      #
      if fullInfo:
         q='(%(idTicket)s, dict(((k,DATA[k]) for k in DATA if %(safeTicket)s), project=IDS[1]%(idProject2)s))'  # noqa: E501
         q=q%QUERY_TEMPLATE
      else:
         q=QUERY_TEMPLATE['idTicket']
      tArr=[k for k,need in (
         ('ticketsOpen', fromOpen),
         ('ticketsTaken', fromTaken),
         ('ticketsClosed', fromClosed)
      ) if need]
      r=[]
      for pId in project:
         for status in tArr:
            r.append(self.db.query(what=q, branch=(userId, pId, status), where=condition, recursive=False))
      return gChain(*r)

   def ticketInfo(self):
      #! реализовать метод для получения данных по конкретному тикету
      raise NotImplementedError()

   def ticketWatch(self, cb, userId=NULL, login=NULL, userIdOfOperator=NULL):
      if userIdOfOperator is not NULL: userId=userIdOfOperator
      assert isStr(userId) and userId
      assert isFunction(cb)
      data={'userId':userId, 'cb':cb}
      if login is not NULL:
         assert isStr(login) and login
         oId=self.operatorId(login)
         o=self.db.get((userId, oId), returnRaw=True)
         if o is None:
            raise LogicError(-304)
         if not o['isActive']:
            raise LogicError(-305)
         data['login']=login
         data['oId']=oId
      self.__hook['ticket'][id(cb)]=data

   def __ticketChanged(self, userId, project, ticket, data=None, changed=None, pId=None, tId=None):
      try:
         hooks=self.__hook['ticket']
         if not hooks: return
         if pId is None:
            pId=self.projectId(project)
         if tId is None:
            tId=self.ticketId(ticket)
         if changed is None: changed='all'
         needLoadData=False
         if data is None:
            raise NotImplementedError()
         elif isinstance(data, tuple):
            needLoadData=True
         else:
            data=data.copy()
            for k in QUERY_TEMPLATE['safeTicket_raw']:
               if k in data: del data[k]
            data['project']=project
         for cbId, o in hooks.iteritems():
            if userId!=o['userId']: continue
            if 'oId' in o:
               if not self.db.isExist((userId, pId, o['oId'])): continue
               #! нехватает проверки на `isActive`
            if needLoadData:
               data=self.db.get(data, returnRaw=True)
               assert data
               data=data.copy()
               for k in QUERY_TEMPLATE['safeTicket_raw']:
                  if k in data: del data[k]
               data['project']=project
               needLoadData=False
            try:
               o['cb'](userId, project, ticket, changed, data)
            except Exception:
               self.workspace.log(1, 'Error in hook-cb "%s" (%s): %s'%(cbId, o, getErrorInfo()))
      except Exception:
         self.workspace.log(0, (userId, project, ticket), getErrorInfo())

   def _showDB(self, **kwargs):
      showDB(self.db, **kwargs)

   def _showStats(self, **kwargs):
      showStats(self.db, **kwargs)

   def _close(self):
      self.db.close()

   def __enter__(self):
      return self

   def __exit__(self, *err):
      self._close()

if __name__ == '__main__':
   if not console.inTerm():
      raise RuntimeError('This test-suite must be runned in tty')
   from flaskJSONRPCServer import flaskJSONRPCServer
   from logger import LoggerLocal
   ErrorHandler()

   workspace=MagicDict()
   workspace.dbPath=getScriptPath(real=True, f=__file__)+'/dbTest'
   # workspace.dbPath=getScriptPath(real=True, f=__file__)+'/db'
   workspace.server=flaskJSONRPCServer(None, gevent=False, log=4, tweakDescriptors=[1000, 1000], experimental=False, controlGC=False)
   workspace.logger=LoggerLocal(app='ScreenDesk-Logic-TestSuite', server=workspace.server, maxLevel=4)
   workspace.logger.settings.allowExternalNull=False
   workspace.logger.settings.lengthSolong=2048
   workspace.log=workspace.logger.log

   s=workspace.server._raw_input('Reinit DB\'s Namespaces? ')=='y'
   logic=Logic(workspace, reinitNamespaces=s)
   logic._showDB(limit=100)
   #

   #
   if workspace.server._raw_input('Run interactive mode? ')=='y': console.interact(scope=locals())
   logic._showStats()
