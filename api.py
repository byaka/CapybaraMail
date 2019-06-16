# -*- coding: utf-8 -*-
from functionsex import *
from VombatiDB import errors as dbError

from errors import *
from store import StoreBase, StoreDB, StoreFilesLocal

class ApiBase(object):
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
      self.store=ClassFactory(StoreBase, (StoreFilesLocal, StoreDB))(self.workspace)

   def start(self, **kwargs):
      self._settings=dict(self.settings)
      self.settings._MagicDictCold__freeze()
      self._supports=dict(self.supports)
      self.supports._MagicDictCold__freeze()
      self.settings_frozen=True
      self._start(**kwargs)
      self.started=True

   def _start(self, **kwargs):
      self.store.start()

class ApiAccaunt(ApiBase):
   def _init(self, **kwargs):
      super(ApiUser, self)._init(**kwargs)

   def accauntAdd(self, login, password, descr=None, avatar=None, connector=None):
      """
      Add new accaunt.

      :param str login: Login (name) of accaunt. This also will ID of accaunt. Can contain any letters - it will be normalized automatically.
      :param str password: Password for accaunt.
      :param str|none descr: Description of accaunt (defaults to None).
      :param str|none avatar: Encoded to base64 image (defaults to None).
      :param tuple|none connector: Config for connectors (defaults to None).
      """
      self.store.userAdd(login, password, descr=descr, avatar=avatar, strictMode=True)

   def connectorAdd(self, login, name, type, config, descr=None):
      """
      Add connector to accaunt (usually connector needed for receive and send messages).

      :param str login: Login of accaunt.
      :param str name: Connector's name, will be ID of connector.
      :param str type: Type of connector, this will be used for find correct connector.
      :param dict config: Parameters for connector.
      :param str|none descr: Description of new connector (defaults to None).
      """
      pass

   def connectorList(self):
      """
      List all supported connectors with format of configs (it contain fields with `name`, `type` and `required` mark).

      :return dict:
      """
      pass

   def connectorSwitch(self, login, name, to=None):
      """
      Switch active-status of specific connector.

      :param str login: Login of accaunt.
      :param str name: Name of connector.
      :param bool|none to: New active-status. If `None`, it will switched to opposite status (defaults to None).
      """
      pass
