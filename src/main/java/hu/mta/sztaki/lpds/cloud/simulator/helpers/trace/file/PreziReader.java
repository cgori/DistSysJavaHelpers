package hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.file;

import java.lang.reflect.InvocationTargetException;

import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;

public class PreziReader extends TraceFileReaderFoundation {

	/**
	 * Constructs a "Prezi" file reader that later on can act as a trace producer
	 * for user side schedulers.
	 * 
	 * @param fileName            The full path to the gwf file that should act as
	 *                            the source of the jobs produced by this trace
	 *                            producer.
	 * @param from                The first job in the gwf file that should be
	 *                            produced in the job listing output.
	 * @param to                  The last job in the gwf file that should be still
	 *                            in the job listing output.
	 * @param allowReadingFurther If true the previously listed "to" parameter is
	 *                            ignored if the "getJobs" function is called on
	 *                            this trace producer.
	 * @param jobType             The class of the job implementation that needs to
	 *                            be produced by this particular trace producer.
	 * @throws SecurityException     If the class of the jobType cannot be accessed
	 *                               by the classloader of the caller.
	 * @throws NoSuchMethodException If the class of the jobType does not hold one
	 *                               of the expected constructors.
	 */

	public PreziReader(String fileName, int from, int to, boolean allowReadingFurther, Class<? extends Job> jobType)
			throws SecurityException, NoSuchMethodException {
		super("Prezi format", fileName, from, to, allowReadingFurther, jobType);
	}

	@Override
	protected boolean isTraceLine(final String line) {
		if (line == null) {
			return false;
		}
		if (!line.contains(" ")) {
			return false;
		}
		
		String[] lineArray = line.split(" "); 
		if(!(lineArray.length == 4)){
			return false;
		}
		
		try {
			try {
				// check the Job arrival time
				Integer.parseInt(lineArray[0]);
				// check if job duration can be parsed toa  float with 1/1000 precision
				Float.parseFloat(lineArray[1]);
				if(lineArray[2].contains(" ")) {
					// Contains whitespace so therefore invalid.
					return false;
				}
				if(!lineArray[3].equals("url") && !lineArray[3].equals("default") && !lineArray[3].equals("export")) {
					// Unknown executable name.
					return false;
				}
			} catch (NumberFormatException e) {
				return false;
			}
		} catch (ArrayIndexOutOfBoundsException e) {
			// Invalid line length
			return false;

		}
		return true;
	}

	@Override
	protected void metaDataCollector(String line) {
		if (line.contains("Processors")) {
			String[] splitLine = line.split("\\s");
			try {
				maxProcCount = parseLongNumber((splitLine[splitLine.length - 1]));
			} catch (NumberFormatException e) {
				// safe to ignore as there is no useful data here then
			}
		}
	}

	@Override
	public Job createJobFromLine(String jobstring)
			throws IllegalArgumentException, InstantiationException, IllegalAccessException, InvocationTargetException {
		String[] lines = jobstring.split(" ");
		try {
			String id = lines[2].trim();
			long submitTimeSecs = Long.parseLong(lines[0]);
			long queueTimeSecs = 0;
			long execTimeSecs = (long) Float.parseFloat(lines[1]);
			int processors = 1;
			double perProcCpu = -1;
			long perProcMem = 512;
			String user = null;
			String group = null;
			String executable = lines[3];
			Job preceding = null;
			long delayAfter = 0;
			return jobCreator.newInstance(id, submitTimeSecs, queueTimeSecs, execTimeSecs, processors, perProcCpu, perProcMem, user, group, executable,
					preceding, delayAfter);
		} catch (ArrayIndexOutOfBoundsException e) {
			e.printStackTrace();
			return null;
		}
	}

	private String parseTextualField(final String unparsed) {
		return unparsed.equals("-1") ? "N/A" : unparsed;
		// unparsed.matches("^-?[0-9](?:\\.[0-9])?$")?"N/A":unparsed;
	}

}
