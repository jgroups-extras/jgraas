#!/bin/bash

D=`dirname $0`
$D/run.sh org.jgroups.jgraas.client.JChannelClient $* $*
