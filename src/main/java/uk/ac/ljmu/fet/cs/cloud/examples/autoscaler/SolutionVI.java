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
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;

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
	public static final double maxUtilisationLevelBeforeNewVM = .7;

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
	 */
	@Override
	public void tick(long fires) {
		final Iterator<String> kinds = vmSetPerKind.keySet().iterator();
		// get each kind of application and create a new VM for that type
		while (kinds.hasNext()) {
			final String kind = kinds.next();
			final ArrayList<VirtualMachine> vmset = vmSetPerKind.get(kind);
				if (vmset.size() < 1) {
					requestVM(kind);
					continue;
				} else {
					//Removing the last vm of its kind check
					if (vmset.size() == 1) {
						final VirtualMachine onlyMachine = vmset.get(0);
						if (onlyMachine.underProcessing.isEmpty() && onlyMachine.toBeAdded.isEmpty()) {
							// Check the last VM is not Processing anything and create a counter to count 
							//how many times in a hour the VM isn't used before destroying
							Integer i = unnecessaryHits.get(onlyMachine);
							if (i == null) {
								unnecessaryHits.put(onlyMachine, 1);
							} else {
								i++;
								if (i < 30) {
									unnecessaryHits.put(onlyMachine, i);
								} else {
									// One hour of the vm not being used we can then destroy it
									unnecessaryHits.remove(onlyMachine);
									destroyVM(onlyMachine);
									kinds.remove();
								}
							}
							// We don't need to check if we need more VMs as it has no computation
							continue;
						}
						unnecessaryHits.remove(onlyMachine);
						// We can now check if any VM's are not processing anything and have been low utilisation level for an hour before removing
					} else {
						boolean destroyed = false;
						for (int counter = 0; counter < vmset.size(); counter++) {
							final VirtualMachine vm = vmset.get(counter);
							if (vm.underProcessing.isEmpty() && vm.toBeAdded.isEmpty()) {
								// The VM has no task on it at the moment, good candidate
								if (getHourlyUtilisationPercForVM(vm) < minUtilisationLevelBeforeDestruction) {
									// The VM's load was under 10% in the past hour
									destroyVM(vm);
									destroyed = true;
									counter--;
								}
							}
						}
						if (destroyed) {
							// No need to check the average workload now, as we just destroyed a VM..
							continue;
						}
					}

					// Check if new VM's need to be added
					double VMsUtilPercent = 0;
					for (VirtualMachine vm : vmset) {
						VMsUtilPercent += getHourlyUtilisationPercForVM(vm);
					}
					if (VMsUtilPercent / vmset.size() > maxUtilisationLevelBeforeNewVM) {
						// Average utilisation of VMs are over threshold, we need a new one
						requestVM(kind);
					}
			}
		}
	}
}