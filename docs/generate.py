# -*- coding: utf-8 -*-
from functionsex import *
import docit

def prepMethodForDocs(o):
   o.data=o.data.replace('(self', '(')
   o.data=o.data.replace(', _conn=None', '')
   o.data=o.data.replace('(, ', '(')
   #
   tArr, o.example=o.example, []
   for oo in tArr:
      if oo.type=='fake_response_hide':
         oo._isFakeResponse=True
         continue
      elif oo.type in ('json', 'badjson', 'fake_response'):
         try:
            s=json.loads(oo.code) if oo.type=='json' else eval(oo.code)
            s=json.dumps(s, indent=3)
            oo.code=s
         except Exception: pass
         oo._isFakeResponse=(oo.type=='fake_response')
         oo.type='json'
      o.example.append(oo)

def main():
   import api
   buildPath=os.path.join(getScriptPath(f=__file__), 'build')
   outputPath=os.path.join(getScriptPath(f=__file__), 'dist')
   ignoreClasses=['ApiBase', 'ApiWrapper']
   ignoreFuncsType=['private', 'public', 'special', 'undoc']

   api=docit.pydoc2api(api).summary(moduleWhitelist=['self'])  #['self', 'self.']
   # sort like in sources
   #! этот код нужно перенести в docit дополнив поддержку
   # https://julien.danjou.info/blog/2015/python-ast-checking-method-declaration
   import ast
   print api.file
   api._ast=ast.parse(fileGet(api.file))
   tArr1={}
   for oo in ast.walk(api._ast):
      if not isinstance(oo, ast.ClassDef): continue
      if oo.name not in api.tree.classes: continue
      tArr1[oo.name]=oo.lineno
      tArr2={}
      for oo2 in oo.body:
         if not isinstance(oo2, ast.FunctionDef): continue
         if oo2.name not in api.tree.classes[oo.name].tree.methods.public: continue
         tArr2[oo2.name]=oo2.lineno
      api.tree.classes[oo.name].tree.methods.publicOrder.sort(key=lambda k: tArr2[k])
   api.tree.classesOrder.sort(key=lambda k: tArr1[k])
   # prepare api
   for k in ignoreClasses:
      if k not in api.tree.classes: continue
      del api.tree.classes[k]
      api.tree.classesOrder.remove(k)
   for k in ignoreFuncsType:
      if k not in api.tree.methods: continue
      api.tree.methods[k]={}
      api.tree.methods[k+'Order']=[]
   #
   for c in api.tree.classes.itervalues():
      # c.data, c._data=c._obj.path, c.data
      # c.name, c._name=c._obj.path, c.name
      c['return'], c._return='', c['return']
      #
      c.tree.methods.special={}
      c.tree.methods.specialOrder=[]
      c.tree.methods.private={}
      c.tree.methods.privateOrder=[]
      #
      for m in c.tree.methods.public.itervalues(): prepMethodForDocs(m)
   fileWrite(os.path.join(outputPath, 'docs/api.json'), reprEx(api, indent=3, sortKeys=True))
   files=docit.api2html(api, os.path.join(outputPath, 'docs/pages'), os.path.join(buildPath, 'docs/template/simpleBootstrapWithToc.html'), hLevel=1)
   print 'Done!'
   for name, path in files.items():
      print '>>', path

if __name__=='__main__':
   sys.path.insert(0, os.path.join(getScriptPath(f=__file__), '..'))
   main()
