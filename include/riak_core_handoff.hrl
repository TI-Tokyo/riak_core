%% -------------------------------------------------------------------
%%
%% riak_core_handoff.hrl:
%%
%% Copyright (c) 2007-2017 Basho Technologies, Inc.
%%               2022 TI-Tokyo  All Rights Reserved.
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

-ifndef(RIAK_CORE_HANDOFF_HRL).
-define(RIAK_CORE_HANDOFF_HRL, included).

-type riak_core_handoff_dict() :: dict:dict().

-define(PT_MSG_INIT, 0).
-define(PT_MSG_OBJ, 1).
-define(PT_MSG_OLDSYNC, 2).
-define(PT_MSG_SYNC, 3).
-define(PT_MSG_CONFIGURE, 4).
-define(PT_MSG_BATCH, 5).

-record(ho_stats,
        {
          interval_end                  :: erlang:timestamp(),
          last_update = os:timestamp()  :: erlang:timestamp(),
          objs=0                        :: non_neg_integer(),
          bytes=0                       :: non_neg_integer()
        }).

-type ho_stats() :: #ho_stats{}.
-type ho_type() :: ownership | hinted | repair | resize.
-type predicate() :: fun((any()) -> boolean()).

-type index() :: chash:index_as_int().
-type mod_src_tgt() :: {module(), index(), index()} | {undefined, undefined, undefined}.
-type mod_partition() :: {module(), index()}.

-record(handoff_status,
        { mod_src_tgt           :: mod_src_tgt(),
          src_node              :: node(),
          target_node           :: node(),
          direction             :: inbound | outbound,
          transport_pid         :: pid(),
          transport_mon         :: reference(),
          timestamp             :: tuple(),
          status                :: any(),
          stats                 :: riak_core_handoff_dict(),
          vnode_pid             :: pid() | undefined,
          vnode_mon             :: reference() | undefined,
          type = undefined      :: ho_type() | undefined,
          req_origin            :: node(),
          filter_mod_fun        :: {module(), atom()} | undefined,
          size = {0, objects}   :: {function(), dynamic} | {non_neg_integer(), bytes | objects}
        }).
-type handoff_status() :: #handoff_status{}.

-type known_handoff() :: {{module(), index()},
                           {ho_type()|'delete',
                            'inbound'|'outbound'|'local',
                            node()|'$resize'|'$delete'}}.

-endif.
