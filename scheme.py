# -*- coding: utf-8 -*-

from functionsex import *

__all__=['SCHEME']

#~ для корректной работы требуется установка флага `linkedChilds_inheritNSFlags=True`

SCHEME=[
#  (Name, (Parents, Childs, Columns, AllowOnlyIndexed[True], AllowOnlyNumerable[False], localAutoIncrement[fromSetts], linkChilds[fromSetts])) # noqa

   ('user', (False, ['node_date', 'node_email', 'node_label'], {
      '_passwordHash':'str',
      '_connector':('list', 'none'),
      'isActive':'bool',
      'name':'str',
      'descr':('str', 'none'),
      'avatar':('str', 'none'),
   }, True, False, False, None)),

   ('node_email', ('user', 'email', False, False, False, False, None)),

   ('email', ('node_email', 'msg', {
      'name':('str', 'none'),
   }, True, False, False, None)),

   ('node_date', ('user', 'date', False, False, False, False, True)),

   ('date', ('node_date', ['node_email', 'node_dialog', 'node_msg'], False, True, True, False, None)),

   ('node_dialog', ('date', 'dialog', False, False, False, False, False)),

   ('dialog', (['node_dialog'], ['msg'], False, True, True, True, None)),

   ('node_msg', ('date', 'msg', False, False, False, False, True)),

   #! ожидает #83 для ограничения вложенности только внутри диалогов
   ('msg', (['node_msg', 'email', 'dialog', 'msg'], ['msg', 'label'], {
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

   ('node_label', ('user', 'label', False, False, False, False, None)),

   ('label', (['node_label', 'label', 'msg'], ['label'], {
      'name':'str',
      'descr':('str', 'none'),
      'color':('str', 'none'),
   }, True, False, False, None)),

   #~ поидеи это легко реализуется поферх основного класса добавлением данных строк в схему и оборачиванием всех методов класс где идет итерация\получение данных. в обертах идет вызов основного метоад, а затем вычисляется к каким иным юзерам есть доступ и вызывается оригинальный метод для них -а результаты всех вызовов склеиваеются одним из способов
   # ('node_shared_user', ('user', 'shared_user', False, False, False, False, True)),
   # ('shared_user', ('node_shared_user', False, {
   #    'contactlistMap':'dict',
   # }, True, False, False, False)),

   # ('node_contactlist', ('user', 'contactlist', False, False, False, False, True)),

   # ('contactlist', ('node_contactlist', ['contact'], {
   #    'name':'str',
   #    'descr':('str', 'none'),
   #    'color':('str', 'none'),
   # }, True, True, True, None)),

   # ('contact', ('contactlist', ['node_msg_in', 'node_msg_out', 'node_field'], {
   #    'nameFirst':'str',
   #    'nameSecond':('str', 'none'),
   #    'avatar':('str', 'none'),
   #    'note':('str', 'none'),
   # }, True, True, True, None)),
]
