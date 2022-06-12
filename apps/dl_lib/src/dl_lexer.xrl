Definitions.

DECL = \.((decl)|(output)|(input)|(type)) 
Const = [a-z][0-9a-zA-Z_]*
Var = [A-Z_][0-9a-zA-Z_]*
WHITESPACE = [\s\t]
COMMA = (,)
EOL = \n|\r\n|\r
BRAC = \(|\)
TERMINATOR = \.
CONNECT = :-
NEG = !
COMMENT = (//)
QUOTE = "


%" this line is to make syntax highlighting work

% D = [0-9]


Rules.


% {D}+ :
%   {token,{integer,TokenLine,list_to_integer(TokenChars)}}.

% {D}+\.{D}+((E|e)(\+|\-)?{D}+)? :
%   {token,{float,TokenLine,list_to_float(TokenChars)}}. 

{DECL}.*{EOL} : skip_token.
{COMMENT}.*{EOL} : skip_token.
{WHITESPACE} : skip_token.
{EOL} : skip_token. 
{Const} :
  {token, {dl_const, TokenLine, TokenChars}}.
{QUOTE}{Const}{QUOTE} : 
  {token, {dl_const, TokenLine, string:trim(TokenChars, both, "\"")}}.
{Var} :
  {token, {dl_var, TokenLine, TokenChars}}.
{COMMA} : {token, {list_to_atom(TokenChars), TokenLine}}.
{BRAC} : {token, {list_to_atom(TokenChars), TokenLine}}.
{CONNECT} : {token, {list_to_atom(TokenChars), TokenLine}}.
{TERMINATOR} : {token, {list_to_atom(TokenChars), TokenLine}}.
{CONNECT} : {token, {list_to_atom(TokenChars), TokenLine}}.
{NEG} : {token, {list_to_atom(TokenChars), TokenLine}}.

Erlang code.
