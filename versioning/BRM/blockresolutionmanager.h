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

/******************************************************************************
 * $Id: blockresolutionmanager.h 1825 2013-01-24 18:41:00Z pleblanc $
 *
 *****************************************************************************/

/** @file 
 * class BlockResolutionManager
 */

#ifndef BLOCKRESOLUTIONMANAGER_H_
#define BLOCKRESOLUTIONMANAGER_H_

#include <sys/types.h>
#include <vector>
#include <set>

#include "brmtypes.h"
#include "mastersegmenttable.h"
#include "extentmap.h"
#include "vbbm.h"
#include "vss.h"
#include "copylocks.h"

#if defined(_MSC_VER) && defined(xxxBLOCKRESOLUTIONMANAGER_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace BRM {

/** @brief The BlockResolutionManager manages the Logical Block ID space.
 *
 * The BlockResolutionManager manages the Logical Block ID space.  Its
 * primary use is to translate <LBID, VerID, VBFlag> triples
 * to <OID, FBO> pairs and vice-versa.
 *
 * @note This class will be used by C code, so it should not throw exceptions.
 */
class BlockResolutionManager {
	public:
		EXPORT explicit BlockResolutionManager(bool ronly = false) throw();
		EXPORT ~BlockResolutionManager() throw();

		/** @brief Persistence API.  Loads the local Extent Map from a file.
		 *
		 * Persistence API.  Loads the <b>local</b> Extent Map from a file.
		 *
		 * @warning The load must be done on each slave node atomically wrt
		 * writing operations, otherwise nodes may be out of synch.
		 * @param filename Relative or absolute path to a file saved with saveExtentMap.
		 * @return 0, throws if EM throws
		 */
		EXPORT int loadExtentMap(const std::string& filename, bool fixFL);

		/** @brief Persistence API.  Saves the local Extent Map to a file.
		 *
		 * Persistence API.  Saves the <b>local</b> Extent Map to a file.
		 *
		 * @param filename Relative or absolute path to save to.
		 * @return 0 on success, throws if EM throws
		 */
		EXPORT int saveExtentMap(const std::string& filename);
		
		/** @brief Persistence API.  Loads all BRM snapshots.
		 *
		 * Loads all <b>local</b> BRM structures from files saved with saveState().
		 *
		 * @warning The load must be done on each slave node atomically wrt
		 * writing operations, otherwise nodes may be out of synch.
		 * @param filename The filename prefix to use.  Loads 4 files with that prefix.
		 * @return 0 on success, -1 on error
		 */
		EXPORT int loadState(std::string filename, bool fixFL=false) throw();

		/** @brief Persistence API.  Loads the BRM deltas since the last snapshot.
		 *
		 * Loads all <b>local</b> BRM structures from files saved with saveState().
		 *
		 * @warning The load must be done on each slave node atomically wrt
		 * writing operations, otherwise nodes may be out of synch.
		 * @param filename The filename prefix to use.  Loads 4 files with that prefix.
		 * @return 0 on success, -1 on error
		 */
		EXPORT int replayJournal(std::string filename) throw();

		/** @brief Persistence API.  Saves all BRM structures.
		 *
		 * Saves all <b>local</b> BRM structures to files.
		 *
		 * @param filename The filename prefix to use.  Saves 4 files with that prefix.
		 * @return 0 on success, -1 on error
		 */
		EXPORT int saveState(std::string filename) throw();

	private:
		explicit BlockResolutionManager(const BlockResolutionManager& brm);
		BlockResolutionManager& operator=(const BlockResolutionManager& brm);
		MasterSegmentTable mst;
		ExtentMap em;
		VBBM vbbm;
		VSS vss;
		CopyLocks copylocks;
		
};

}

#undef EXPORT

#endif 
