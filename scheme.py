# -*- coding: utf-8 -*-

from functionsex import *

__all__=['SCHEME']

#~ для корректной работы требуется установка флага `linkedChilds_inheritNSFlags=True`

SCHEME=[
#  (Name, (Parents, Childs, Columns, AllowOnlyIndexed[True], AllowOnlyNumerable[False], localAutoIncrement[fromSetts], linkChilds[fromSetts])) # noqa

   ('user', (False, ['node_label', 'node_date', 'node_contactlist', 'node_shared_user'], {
      '_passwordHash':'str',
      '_connector':('list', 'none'),
      'isActive':'bool',
      'name':'str',
      'descr':('str', 'none'),
      'avatar':('str', 'none'),
   }, True, False, False, None)),

   ('node_shared_user', ('user', 'shared_user', False, False, False, False, True)),
   ('shared_user', ('node_shared_user', False, {
      'contactlistMap':'dict',
   }, True, False, False, False)),

   ('node_label', ('user', 'label', False, False, False, False, True)),
   ('node_date', ('user', 'date', False, False, False, False, True)),
   ('node_contactlist', ('user', 'contactlist', False, False, False, False, True)),

   ('contactlist', ('node_contactlist', ['contact'], {
      'name':'str',
      'descr':('str', 'none'),
      'color':('str', 'none'),
   }, True, True, True, None)),

   ('contact', ('contactlist', ['node_msg_in', 'node_msg_out', 'node_field'], {
      'nameFirst':'str',
      'nameSecond':('str', 'none'),
      'avatar':('str', 'none'),
      'note':('str', 'none'),
   }, True, True, True, None)),
   #! нет поиска контактов по полям

   ('date', ('node_date', ['node_msg_in', 'node_msg_out', 'node_dialog'], False, True, True, False, None)),

   ('node_dialog', ('date', 'dialog', False, False, False, False, False)),
   ('node_msg_in', (['contact', 'date'], 'msg', False, False, False, False, True)),
   ('node_msg_out', (['contact', 'date'], 'msg', False, False, False, False, True)),
   ('node_field', ('contact', ['field_email', 'field_phone', 'field_custom'], False, False, False, False, True)),

   #! внутри лейбла сообщения никак не масштабируются
   ('label', (['node_label', 'label'], ['label', 'msg'], {
      'name':'str',
      'descr':('str', 'none'),
      'color':('str', 'none'),
   }, True, True, True, None)),

   #! ожидает #83 для ограничения вложенности только внутри диалогов
   ('msg', (['node_msg_in', 'node_msg_out', 'label', 'dialog', 'field_email', 'msg'], 'msg', {
      'subject':'str',
      'timestamp':'datetime',
      'isIncoming':'bool',
      'from':'str',
      'to':('tuple', 'none'),
      'cc':('tuple', 'none'),
      'bcc':('tuple', 'none'),
      'raw':'str',
      'body':'str',
      'attachments':('tuple', 'none'),
   }, True, False, False, None)),

   ('dialog', (['node_dialog'], ['msg'], False, True, True, True, None)),

   ('field_email', ('contact', 'msg', {
      'value':'str',
      'name':('str', 'none'),
      'descr':('str', 'none'),
   }, True, False, False, True)),

]
