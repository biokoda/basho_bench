%% -------------------------------------------------------------------
%%
%% basho_bench_driver_actordb: Driver for ActorDB
%%
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(basho_bench_driver_actordb).
-export([new/1,
		genval/1,
         run/4]).


genval(_) ->
	fun() -> <<"AAALJSDA111LKDLKADJAOIDJ5452871034AD236457JAAHDKAJDN">> end.

-record(dp,{pid,id}).
new(Id) ->
	Hosts = basho_bench_config:get(actordb_mysql_addresses, ["127.0.0.1:33307"]),
	TargetAdr = lists:nth((Id rem length(Hosts)+1), Hosts),
	[Host,Port] = string:tokens(TargetAdr,":"),
	MyOpt = [{host,Host},{port,list_to_integer(Port)},{user,"user"},{password,"password"},{database,"actordb"}],
	{ok,Pid} = mysql:start_link(MyOpt),

    {ok, #dp{pid = Pid, id = integer_to_binary(Id)}}.

run(get, KeyGen, _ValueGen, P) ->
	Key = integer_to_binary(KeyGen()),
    {ok, _ColumnNames, _Rows} = mysql:query(P#dp.pid, <<"actor type1(",(P#dp.id)/binary,") create;SELECT * FROM tab WHERE id=",Key/binary>>),
    {ok,P};

run(put, KeyGen, ValueGen, P) ->
    % Object = {KeyGen(), ValueGen()},
    Key = integer_to_binary(KeyGen()),
    ok = mysql:query(P#dp.pid, <<"actor type1(",(P#dp.id)/binary,") create;INSERT INTO tab VALUES (",Key/binary, ",'",(ValueGen())/binary,"',","1)">>),
    {ok, P}.

