/*
 *  ========================================================================
 *  Helper classes to support simulations of large scale distributed systems
 *  ========================================================================
 *  
 *  This file is part of DistSysJavaHelpers.
 *  
 *    DistSysJavaHelpers is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation, either version 3 of the License, or
 *    (at your option) any later version.
 *
 *   DistSysJavaHelpers is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 *  (C) Copyright 2012-2015, Gabor Kecskemeti (kecskemeti.gabor@sztaki.mta.hu)
 */

package hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.random;

import java.util.ArrayList;
import java.util.List;

import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;
import hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.TraceManagementException;

/**
 * A trace producer that generates a trace on the fly with random data. The
 * generated trace can be characterized with several properties. Please note
 * that even omitting one property during the class' setup will result in an
 * exception.
 * 
 * @author "Gabor Kecskemeti, Laboratory of Parallel and Distributed Systems, MTA SZTAKI (c) 2012-5"
 */
public class RepetitiveRandomTraceGenerator extends GenericRandomTraceGenerator {

	/**
	 * These are the main characteristics of the generated trace. If even one of
	 * them end up being -1 by the time the trace is supposed to be generated,
	 * then the trace generation will not proceed further. For details of each
	 * individual property, see their getter and setter functions.
	 */
	private int parallel = -1, maxStartSpread = -1, execmin = -1, execmax = -1, mingap = -1, maxgap = -1,
			minNodeProcs = -1, maxNodeprocs = -1;

	/**
	 * The time instance when the trace will start.
	 */
	private long submitStart = 0;

	/**
	 * Determines how many jobs can run in parallel at any given time. (this is
	 * a maximum number, the actual trace might not contain any parallel
	 * fragments that high in parallelism)
	 * 
	 * @return the current level of maximum parallelism.
	 */
	public int getParallel() {
		return parallel;
	}

	/**
	 * Sets a new level of parallelism for jobs in the newly generated traces.
	 * 
	 * @param parallel
	 *            the maximum number of jobs allowed in parallel in any part of
	 *            the generated trace.
	 */
	public void setParallel(int parallel) {
		this.parallel = parallel;
		try {
			regenJobs();
		} catch (TraceManagementException e) {
			// ignore.
		}
	}

	/**
	 * The starting time of any job in a parallel section can be dispersed over
	 * a given period of time. This time period is specified here.
	 * 
	 * @return The range (specified in seconds) over which the current trace
	 *         spreads the start events for all jobs in a parallel section.
	 */
	public int getMaxStartSpread() {
		return maxStartSpread;
	}

	/**
	 * The function allows to set a new spread value for the start time of the
	 * jobs in every parallel section generated by the current trace generator.
	 * 
	 * @param maxStartSpread
	 *            over how many seconds should be dispersed the start events of
	 *            each job in a parallel section
	 */
	public void setMaxStartSpread(int maxStartSpread) {
		this.maxStartSpread = maxStartSpread;
		try {
			regenJobs();
		} catch (TraceManagementException e) {
			// ignore.
		}
	}

	/**
	 * Retrieves the minimum execution time of a job
	 * 
	 * @return the number of seconds a job should be minimally occupying a CPU
	 *         with 100% load
	 */
	public int getExecmin() {
		return execmin;
	}

	/**
	 * Sets the shortest execution time for any job in the trace
	 * 
	 * @param execmin
	 *            the number of seconds a job should be minimally occupying a
	 *            CPU with 100% load
	 */
	public void setExecmin(int execmin) {
		this.execmin = execmin;
		try {
			regenJobs();
		} catch (TraceManagementException e) {
			// ignore.
		}
	}

	/**
	 * Retrieves the maximum possible execution time for a job
	 * 
	 * @return the number of seconds a job should be maximally occupying a CPU
	 *         with 100% load
	 */
	public int getExecmax() {
		return execmax;
	}

	/**
	 * Sets the longest possible execution time for any job in the trace
	 * 
	 * @param execmin
	 *            the number of seconds a job should be maximally occupying a
	 *            CPU with 100% load
	 */
	public void setExecmax(int execmax) {
		this.execmax = execmax;
		try {
			regenJobs();
		} catch (TraceManagementException e) {
			// ignore.
		}
	}

	/**
	 * The minimum gap between two parallel sections. The duration of a parallel
	 * section is calculated as the sum of the maximum starting spread and the
	 * maximum job length. Thus the next parallel section after the currently
	 * ongoing one will be initiated by the trace generator after the duration
	 * of the previous section and the length of the gap between the two
	 * sections.
	 * 
	 * @return the absolute minimum duration (in seconds) that should be spent
	 *         before a new parallel section can be started in the generated
	 *         trace
	 */
	public int getMingap() {
		return mingap;
	}

	/**
	 * Sets the minimum gap. See the getMingap function for details.
	 * 
	 * @param mingap
	 *            the new minimum duration (in seconds) to be spent without
	 *            tasks between parallel sections.
	 */
	public void setMingap(int mingap) {
		this.mingap = mingap;
		try {
			regenJobs();
		} catch (TraceManagementException e) {
			// ignore.
		}
	}

	/**
	 * The maximum gap between parallel sections. See the getMingap function for
	 * details.
	 * 
	 * @return the maximum duration (in seconds) that could be past without
	 *         having any activities submitted to the system between two
	 *         parallel sections.
	 */
	public int getMaxgap() {
		return maxgap;
	}

	/**
	 * Sets the maximum gap between parallel sections. See the getMingap
	 * function for details.
	 * 
	 * @param maxgap
	 *            the maximum jobless gap in seconds between two parallel
	 *            sections.
	 */
	public void setMaxgap(int maxgap) {
		this.maxgap = maxgap;
		try {
			regenJobs();
		} catch (TraceManagementException e) {
			// ignore.
		}
	}

	/**
	 * The minimum number of processors to be used by single job.
	 * 
	 * Note: If the level of maximum parallelism is not reached by in the
	 * current parallel section, but maxtotalprocs has been already reached then
	 * this minimum number of processors are assigned for the rest of the newly
	 * generated jobs in the current parallel section.
	 * 
	 * @return currently set minimum processors limit
	 */
	public int getMinNodeProcs() {
		return minNodeProcs;
	}

	/**
	 * The minimum number of processors to be used by single job.
	 * 
	 * @param minNodeProcs
	 *            new minimum processors limit
	 */
	public void setMinNodeProcs(int minNodeProcs) {
		this.minNodeProcs = minNodeProcs;
		try {
			regenJobs();
		} catch (TraceManagementException e) {
			// ignore.
		}
	}

	/**
	 * The maximum number of processors to be used by a single job.
	 * 
	 * @return the currently set maximum processor count
	 */
	public int getMaxNodeprocs() {
		return maxNodeprocs;
	}

	/**
	 * The maximum number of processors to be used by a single job.
	 * 
	 * Note: The maximum can temporarily drop to the minimum processor limit.
	 * For details see getMinNodeProcs().
	 * 
	 * @param maxNodeprocs
	 *            the newly required maximum processor count
	 */
	public void setMaxNodeprocs(int maxNodeprocs) {
		this.maxNodeprocs = maxNodeprocs;
		try {
			regenJobs();
		} catch (TraceManagementException e) {
			// ignore.
		}
	}

	/**
	 * Warning: The construction of this object will not result in a readily
	 * usable component. Please make sure that all setters of this class are
	 * used after the object's creation. These setters will allow the generator
	 * to know the characteristics of the required trace.
	 * 
	 * @param jobType
	 *            The class of the job implementation that needs to be produced
	 *            by this particular trace producer.
	 * @throws SecurityException
	 *             If the class of the jobType cannot be accessed by the
	 *             classloader of the caller.
	 * @throws NoSuchMethodException
	 *             If the class of the jobType does not hold one of the expected
	 *             constructors.
	 */
	public RepetitiveRandomTraceGenerator(Class<? extends Job> jobType)
			throws SecurityException, NoSuchMethodException {
		super(jobType);
	}

	/**
	 * The main trace generator function. It's purpose is to construct the trace
	 * characterized by the values acquired through the object's setters (for
	 * details how the trace is generated please see the documentation of each
	 * getter and setter pair). The trace output is written to the
	 * currentlyGenerated list.
	 * 
	 * @throws RuntimeException
	 *             when the trace generator is not initialized properly or when
	 *             the job object cannot be created with the constructor
	 *             specified during the initialization of this trace producer.
	 */
	protected List<Job> generateJobs() {
		try {
			System.err.println("Repetitive Random Trace Generator starts with parameters (JN: " + getJobNum()
					+ ", parallel: " + parallel + ", startSpr: " + maxStartSpread + ", exec: " + execmin + "-" + execmax
					+ ", gap: " + mingap + "-" + maxgap + ", nodeprocs: " + minNodeProcs + "-" + maxNodeprocs
					+ ", totalProcs: " + getMaxTotalProcs() + ")");
			final int execspace = execmax - execmin;
			final int gapspace = maxgap - mingap;
			final int nodeSpace = maxNodeprocs - minNodeProcs;
			final List<Job> generatedList = new ArrayList<Job>(getJobNum());
			for (int i = 0; i < getJobNum() / parallel; i++) {
				int usedProcs = 0;
				long currentMaxTime = submitStart;
				for (int j = 0; j < parallel; j++) {
					final long submittime = submitStart + (maxStartSpread == 0 ? 0 : r.nextInt(maxStartSpread));
					int nprocs = minNodeProcs + (nodeSpace == 0 ? 0 : r.nextInt(nodeSpace));
					nprocs = Math.min(getMaxTotalProcs() - usedProcs, nprocs);
					nprocs = nprocs <= 0 ? 1 : nprocs;
					final long exectime = execmin + (execspace == 0 ? 0 : r.nextInt(execspace));
					usedProcs += nprocs;
					generatedList.add(
							jobCreator.newInstance(null, submittime, 0, exectime, nprocs, -1, -1, "", "", "", null, 0));
					currentMaxTime = Math.max(currentMaxTime, submittime + exectime);
				}
				submitStart = currentMaxTime + mingap + (gapspace == 0 ? 0 : r.nextInt(gapspace));
			}
			return generatedList;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Extends the original isPrepared function to ensure that all setters are
	 * used before traces are generated with this class
	 */
	@Override
	protected boolean isPrepared() {
		return super.isPrepared() && parallel >= 0 && maxStartSpread >= 0 && execmin >= 0 && execmax >= 0 && mingap >= 0
				&& maxgap >= 0 && minNodeProcs >= 0 && maxNodeprocs >= 0;
	}
}
