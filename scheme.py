# -*- coding: utf-8 -*-

from functionsex import *

__all__=['SCHEME']

#~ для корректной работы требуется установка флага `linkedChilds_inheritNSFlags=True`

SCHEME=[
#  (Name, (Parents, Childs, Columns, AllowOnlyIndexed[True], AllowOnlyNumerable[False], localAutoIncrement[fromSetts], linkChilds[fromSetts])) # noqa

   ('user', (False, ['node_label', 'node_dialog', 'node_date', 'node_contactlist', 'node_shared_user'], {
      '_passwordHash':'str',
      'isActive':'bool',
      'name':'str',
      'descr':'str',
      'connectorSettings':'dict',
      'connectorType':'str',
      'avatar':'str',
   }, True, False, False, False)),

   ('node_shared_user', ('user', 'shared_user', False, False, False, False, True)),
   ('shared_user', ('node_shared_user', False, {
      'contactlistMap':'dict',
   }, True, False, False, False)),

   ('node_label', ('user', 'label', False, False, False, False, True)),
   ('node_dialog', ('user', 'dialog', False, False, False, False, True)),  #? возможно стоит сразу заложить масштабирование
   ('node_date', ('user', 'date', False, False, False, False, True)),
   ('node_contactlist', ('user', 'contactlist', False, False, False, False, True)),

   ('contactlist', ('node_contactlist', ['contact'], {
      'name':'str',
      'descr':'str',
      'color':'str',
   }, True, True, True, False)),

   ('contact', ('contactlist', ['node_msg_in', 'node_msg_out', 'node_field'], {
      'nameFirst':'str',
      'nameSecond':'str',
      'avatar':'str',
      'note':'str',
   }, True, True, True, False)),
   #! нет поиска контактов по полям

   ('node_msg_in', ('contact', 'msg', False, False, False, False, True)),
   ('node_msg_out', ('contact', 'msg', False, False, False, False, True)),
   ('node_field', ('contact', ['field_email', 'field_phone', 'field_custom'], False, False, False, False, True)),

   ('date', ('node_date', ['msg'], False, True, True, False, None)),

   ('label', (['node_label', 'label'], ['label', 'msg'], {
      'name':'str',
      'descr':'str',
      'color':'str',
   }, True, True, True, None)),

   #? нужна ли возможность архивировать отдельные сообщения, или только диалоги целеком
   ('msg', (['label', 'dialog', 'date', 'field_email', 'msg'], False, {
      'subject':'str',
      'timestamp':'datetime',
      'isIncoming':'bool',
      'from':'str',
      'to':'tuple',
      'copy':'tuple',
      'copyHidden':'tuple',
      'raw':'str',
      'data':'str',
      'attachments':'tuple',
   }, True, True, False, None)),

   ('dialog', (['node_dialog'], ['msg'], False, True, True, False, None)),  #! нумеруются глобальным автоинкрементом?
   #! не реализована древовидность. вкладывать `msg` друг в друга очень плохая мысль, возможно стоит разбивать на поддиалоги и квладывать их

]
