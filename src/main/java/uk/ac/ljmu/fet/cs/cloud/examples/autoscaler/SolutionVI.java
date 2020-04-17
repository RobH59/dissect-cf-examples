/*
 *  ========================================================================
 *  DISSECT-CF Examples
 *  ========================================================================
 *  
 *  This file is part of DISSECT-CF Examples.
 *  
 *  DISSECT-CF Examples is free software: you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License as published
 *  by the Free Software Foundation, either version 3 of the License, or (at
 *  your option) any later version.
 *  
 *  DISSECT-CF Examples is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of 
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with DISSECT-CF Examples.  If not, see <http://www.gnu.org/licenses/>.
 *  
 *  (C) Copyright 2019, Gabor Kecskemeti (g.kecskemeti@ljmu.ac.uk)
 */
package uk.ac.ljmu.fet.cs.cloud.examples.autoscaler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.PhysicalMachine.ResourceAllocation;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VMManager.VMManagementException;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine.StateChangeException;
import hu.mta.sztaki.lpds.cloud.simulator.io.NetworkNode.NetworkException;

/**
 * The class applies a simple threshold based scaling mechanism: it removes VMs
 * which are not utilised to a certain level; and adds VMs to the VI if most of
 * the VMs are too heavily utilised.
 * 
 * @author "Gabor Kecskemeti, Department of Computer Science, Liverpool John
 *         Moores University, (c) 2019"
 */
public class SolutionVI extends VirtualInfrastructure {
	/**
	 * Minimum CPU utilisation percentage of a single VM that is still acceptable to
	 * keep in the virtual infrastructure.
	 */
	public static final double minUtilisationLevelBeforeDestruction = .1;
	/**
	 * Maximum average CPU utilisation percentage of all VMs of a particular kind of
	 * executable that is still considered acceptable (i.e., under this utilisation,
	 * the virtual infrastructure does not need a new VM with the same kind of
	 * executable).
	 */
	public static final double maxUtilisationLevelBeforeNewPoolInc = .80;
	public static final double maxUtilisationLevelBeforeNewVM = .65;
	public HashMap<String, Integer> poolSize = new HashMap<String, Integer>();

	/**
	 * We keep track of how many times we found the last VM completely unused for an
	 * particular executable
	 */
	private final HashMap<VirtualMachine, Integer> unnecessaryHits = new HashMap<VirtualMachine, Integer>();

	/**
	 * Initialises the auto scaling mechanism
	 * 
	 * @param cloud the physical infrastructure to use to rent the VMs from
	 */
	public SolutionVI(final IaaSService cloud) {
		super(cloud);
	}

	private boolean startup = true;

	/**
	 * The auto scaling mechanism that is run regularly to determine if the virtual
	 * infrastructure needs some changes. The logic is the following:
	 * <ul>
	 * <li>if a VM has less than a given utilisation, then it is destroyed (unless
	 * it is the last VM for a given executable)</li>
	 * <li>if a VM is the last for a given executable, it is given an hour to
	 * receive a new job before it is destructed. <i>After this, one has to
	 * re-register the VM kind to receive new VMs.</i></li>
	 * <li>if an executable was just registered, it will receive a single new
	 * VM.</li>
	 * <li>if all the VMs of a given executable experience an average utilisation of
	 * a given minimum value, then a new VM is created.</li>
	 * </ul>
	 * 
	 * @throws NetworkException
	 * @throws VMManagementException
	 */
	@Override
	public void tick(long fires) {
		Iterator<String> kinds = vmSetPerKind.keySet().iterator();
		//On start up define poolSize for each kind
		if (this.startup) {
			while (kinds.hasNext()) {
				String kind = kinds.next();
				poolSize.put(kind, 3);
				for (int i = 0; i < 3; i++) {
					requestVM(kind);
				}
			}
			this.startup = false;
		}
		while (kinds.hasNext()) {
			String kind = kinds.next();
			ArrayList<VirtualMachine> vmset = vmSetPerKind.get(kind);
			//Check if the last VM is not being used
			if (vmset.size() == 1) {
				VirtualMachine lastVM = vmset.get(0);
				if (lastVM.underProcessing.isEmpty() && lastVM.toBeAdded.isEmpty()) {
					//Wait one hour in ticks to see if the vm receives a job if not remove vm
					if (hitIncrement(lastVM, 30)) {
						kinds.remove();
					} else {
						unnecessaryHits.remove(lastVM);
					}

				}
				//Check the pool to see if any VM's aren't being utilised 
			} else if (vmset.size() == poolSize.get(kind)) {
				for (int i = 0; i < vmset.size(); i++) {
					VirtualMachine lastPoolVMs = vmset.get(i);
					if (getHourlyUtilisationPercForVM(lastPoolVMs) < minUtilisationLevelBeforeDestruction) {
						//20 minute timer to check if the pool size should decrease
						if (hitIncrement(lastPoolVMs, 10)) {
							Integer temp = poolSize.get(kind);
							temp--;
							poolSize.put(kind, temp);
							i--;
						}
					} else {
						unnecessaryHits.remove(lastPoolVMs);
					}
				}

			} else {
				//Check if the added VM's should be removed
				for (int i = 0; i < vmset.size(); i++) {
					VirtualMachine VMs = vmset.get(i);
					if (VMs.underProcessing.isEmpty() && VMs.toBeAdded.isEmpty()) {
						if (getHourlyUtilisationPercForVM(VMs) < minUtilisationLevelBeforeDestruction) {
							//Wait 2 ticks before removing the VM
							if (hitIncrement(VMs, 2)) {
								i--;
							}
						} else {
							unnecessaryHits.remove(VMs);
						}
					} else {
						unnecessaryHits.remove(VMs);
					}
				}
			}
			// Adding VM's
			double subHourUtilSum = 0;
			for (VirtualMachine vm : vmset) {
				subHourUtilSum += getHourlyUtilisationPercForVM(vm);
			}
			//If above MaxutilPool add a new VM to the pool to deal with the overload
			if (subHourUtilSum / vmset.size() > maxUtilisationLevelBeforeNewPoolInc) {
				Integer temp = poolSize.get(kind);
				temp++;
				poolSize.put(kind, temp);
				requestVM(kind);
				//Else add a normal VM which will be removed shortly after
			} else if (subHourUtilSum / vmset.size() > maxUtilisationLevelBeforeNewVM) {
				requestVM(kind);
			}

		}
	}
	
	
	/**
	 * This method is used to count hits before removing a VM
	 * 
	 * @param VirtualMachine, Integer
	 */
	private boolean hitIncrement(VirtualMachine lastVM, int hits) {
		Integer hitIncrement = unnecessaryHits.get(lastVM);
		if (hitIncrement == null)
			unnecessaryHits.put(lastVM, 1);
		else {
			hitIncrement++;
			if (hitIncrement < hits) {
				unnecessaryHits.put(lastVM, hitIncrement);
			} else {
				unnecessaryHits.remove(lastVM);
				destroyVM(lastVM);
				return true;
			}
		}
		return false;
	}
}