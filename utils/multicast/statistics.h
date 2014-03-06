/* Copyright (C) 2014 InfiniDB, Inc.

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; version 2 of
   the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

#ifndef STATISTICS_H
#define STATISTICS_H


typedef struct receiver_stats *receiver_stats_t;
typedef struct sender_stats *sender_stats_t;

#define allocReadStats udpc_allocReadStats
#define receiverStatsStartTimer udpc_receiverStatsStartTimer
#define displayReceiverStats udpc_displayReceiverStats

receiver_stats_t udpc_allocReadStats(int fd, long statPeriod,
				     int printUncompressedPos);
void udpc_receiverStatsStartTimer(receiver_stats_t);
void udpc_displayReceiverStats(receiver_stats_t, int isFinal);

#define allocSenderStats udpc_allocSenderStats
#define displaySenderStats udpc_displaySenderStats

sender_stats_t udpc_allocSenderStats(int fd, FILE *logfile, long bwPeriod,
				     long statPeriod, int printUncompressedPos);
void udpc_displaySenderStats(sender_stats_t,int blockSize, int sliceSize,
			     int isFinal);

#endif
