/*
* Copyright (c) 2013-2015 Software AG, Darmstadt, Germany 
* and/or Software AG USA Inc., Reston, VA, USA, and/or 
* its subsidiaries and or/its affiliates and/or their 
* licensors.
* Use, reproduction, transfer, publication or disclosure 
* is prohibited except as specifically provided for in your 
* License Agreement with Software AG.
*/

package mabh.mr.wc;

import java.util.List;

public interface MapLogic<KI, VI, KR, VR> {
	public List<TuplePair<KR, VR>> execute(KI key, VI value);
}
