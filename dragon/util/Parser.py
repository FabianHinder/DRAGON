from dragon.stream.Structure import Pipeline, Fork
from dragon.stream.StreamObject import Map, Filter
from dragon.stream.WindowChunker import WindowChunker


from dragon.stream.SlidingWindow import SlidingWindow
from dragon.stream.GrowingWindow import GrowingWindow
from dragon.stream.StaticWindow import StaticWindow

_window_types = dict()
_window_types["slide"] = SlidingWindow
_window_types["grow"] = GrowingWindow
_window_types["static"] = StaticWindow


try:
    from lark import Lark, Transformer
    from lark.lexer import Token

    gram = r"""
unnamed_param: param
named_param: PARAM_NAME("="|":")param
params: (unnamed_param ("," unnamed_param)* | (unnamed_param ",")* named_param ("," named_param)* )?
window: "[" (REF_NAME ":")? CLAZZ_NAME params "]"
map:    "(" FUN_NAME fun_param_resolve fun_put params ")"
filter: "<" FUN_NAME fun_param_resolve params ">"
chunk:  "<<" params ")"
pipe: "[" (REF_NAME ":")? pipe_sequence params "]"
forked: "|" fork ("," fork)+ "}"
fork:  forked "<-" pipe_sequence | forked | pipe_sequence
main: fork
pipe_sequence:  pipe_element ("<-" pipe_element)*
pipe_element: window | map | filter | chunk | pipe | "--"

fun_param_resolve: (RESOLVE|"at" PARAM_NAME)?
fun_put : ("put" PARAM_NAME)?

RESOLVE : "resolve"

param: | INTEGER -> integer  
       | SIGNED_NUMBER -> number
       | ESCAPED_STRING -> string
       | "True" -> true
       | "False" -> false
REF_NAME: LITERAL
CLAZZ_NAME: LITERAL
PARAM_NAME: LITERAL
FUN_NAME: LITERAL

LITERAL: /[A-Za-z_][A-Ta-z0-9_]*/
INTEGER : /-?[1-9][0-9]*|0/

%import common.INTEGER
%import common.ESCAPED_STRING
%import common.SIGNED_NUMBER
%import common.WS
%ignore WS
"""

    class LibFilter(Filter):
        def __init__(self, function_lib, func_name, resolve, params):
            super().__init__(self.predicate)
            self.function_lib = function_lib
            self.func_name = func_name
            self.resolve = resolve
            self.params = params
        def __repr__(self):
            return "<"+self.func_name+">"
        def predicate(self, data):
            if self.resolve is not None:
                data = {self.resolve: data}
            self.function_lib[self.func_name](**(self.params | data))
    class LibMap(Map):
        def __init__(self, function_lib, func_name, resolve, put, params):
            super().__init__(self.func)
            self.function_lib = function_lib
            self.func_name = func_name
            self.resolve = resolve
            self.put = put
            self.params = params
        def __repr__(self):
            return "("+self.func_name+")"
        def func(self, data):
            if self.resolve is not None:
                data = {self.resolve: data}
            result = self.function_lib[self.func_name](**(self.params | data))
            if self.put is not None:
                data[self.put] = result
                return data
            else:
                return result
    
    def parse(WAL_code, function_lib = dict()):
        class TreeToWindowStructure(Transformer):
            def unnamed_param(self, s):
                assert len(s) == 1
                s = s[0]
                if type(s) not in [str,int,float,bool]: ## Don't know why but sometimes LARK parses an empty string as param
                    return None
                return (None,s)
            def named_param(self, s):
                assert len(s) == 2
                return (s[0].value,s[1])
            def params(self, s):
                res_unnamed, res_named = [], {}
                for n,v in filter(lambda e: e is not None, s):
                    if n is None:
                        res_unnamed.append(v)
                    else:
                        res_named[n] = v
                return ("parameter",{"unnamed":res_unnamed,"named":res_named})
         
                
            def window(self, s):
                window_desc = dict(map(lambda e: self.resolve(e) if type(e) is Token else e, s))
                window_desc["parameter"]["named"]["name"] = window_desc["REF_NAME"] if "REF_NAME" in window_desc else None ## TODO would be better to actually check this does not overwrite etc.
                return ("sequence",[_window_types[window_desc["CLAZZ_NAME"]](*window_desc["parameter"]["unnamed"],**window_desc["parameter"]["named"])])
        
            def pipe_element(self, s):
                return s[0] if len(s) > 0 else ("sequence",[])
        
            def pipe_sequence(self, s):
                r = []
                for x in s:
                    r+=x[1]
                return ("sequence",r)
        
            def pipe(self, s):
                window_desc = dict(map(lambda e: self.resolve(e) if type(e) is Token else e, s))
                if len(window_desc["sequence"]) == 0:
                    return ("sequence",[])
                if len(window_desc["parameter"]["unnamed"]) > 0:
                    raise ValueError("Unnamed parameters in pipe not allowed")
                window_desc["parameter"]["named"]["name"] = window_desc["REF_NAME"] if "REF_NAME" in window_desc else None ## TODO would be better to actually check this does not overwrite etc.
                p = Pipeline(*window_desc["sequence"],**window_desc["parameter"]["named"])
                return ("sequence",[p])
            def forked(self, s):
                s = [x for _,x in s if len(x[1]) > 0]
                if len(s) <= 1:
                    return ("sequence",s)
                elif len(s) > 1:
                    return ("sequence",[Fork(*s)])
            def fork(self, s):
                r = []
                for x in s:
                    r+=x[1]
                return ("sequence",r)
        
            def fun_param_resolve(self,s):
                if len(s) == 0 or s[0].type == "RESOLVE":
                    return ("resolve",None)
                else:
                    return ("resolve",s[0].value)
            def fun_put(self,s):
                if len(s) == 0:
                    return ("put",None)
                else:
                    return ("put",s[0].value)
            
            def map(self,s):
                window_desc = dict(map(lambda e: self.resolve(e) if type(e) is Token else e, s))
                assert len(window_desc["parameter"]["unnamed"]) == 0
                return ("sequence",[LibMap(function_lib, window_desc["FUN_NAME"], window_desc["resolve"], window_desc["put"], window_desc["parameter"]["named"])])
            def filter(self,s):
                window_desc = dict(map(lambda e: self.resolve(e) if type(e) is Token else e, s))
                assert len(window_desc["parameter"]["unnamed"]) == 0
                return ("sequence",[LibFilter(function_lib, window_desc["FUN_NAME"], window_desc["resolve"], window_desc["parameter"]["named"])])
            def chunk(self,s):
                window_desc = dict(map(lambda e: self.resolve(e) if type(e) is Token else e, s))
                assert len(window_desc["parameter"]["named"]) == 0 and len(window_desc["parameter"]["unnamed"]) == 1
                chunk_size = window_desc["parameter"]["unnamed"][0]
                assert type(chunk_size) is int and chunk_size >= 1
                return ("sequence",[WindowChunker(chunk_size)])
                
            def main(self,s):
                r = []
                for x in s:
                    r+=x[1]
                if len(r) == 0:
                    return None
                elif len(r) == 1:
                    return r[0]
                elif len(r) > 1:
                    return Pipeline(*r)
            
            def resolve(self, s):
                return s.type, s.value
            
            def string(self, s):
                (s,) = s
                return s[1:-1] ## TODO that is no proper unescape of a string!
            def number(self, n):
                (n,) = n
                return float(n)
            def integer(self, n):
                (n,) = n
                return int(n)
            true = lambda self, _: True
            false = lambda self, _: False
        
        return TreeToWindowStructure().transform(Lark(gram, start="main").parse(  WAL_code  ))

except ModuleNotFoundError as e:
    def parse(WAL_code):
        print(f"Cannot parse as LARK (https://github.com/lark-parser/lark) is not installed. Please install LARK via 'pip install lark --upgrade' and try again")