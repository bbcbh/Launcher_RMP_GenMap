package sim;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.InvalidPropertiesFormatException;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import map.Map_Location_Mobility;
import random.MersenneTwisterRandomGenerator;
import random.RandomGenerator;

public class Runnable_ContactMap_Generation_RMP implements Runnable {

	File propFile;
	long seed;

	public Runnable_ContactMap_Generation_RMP(File propFile, long cmap_seed) {
		this.propFile = propFile;
		this.seed = cmap_seed;
	}

	@Override
	public void run() {
		try {
			generate_cMap(this.propFile, this.seed);
		} catch (Exception e) {
			e.printStackTrace(System.err);
		}

	}

	public static void main(String[] args) throws IOException {
		final String USAGE_INFO = String.format("Usage: java %s PROP_FILE_DIRECTORY" + "\n",
				Runnable_ContactMap_Generation_RMP.class.getName());

		if (args.length != 1) {
			System.out.println(USAGE_INFO);
			System.exit(0);
		}

		File baseDir = new File(args[0]);
		File propFile = new File(baseDir, SimulationInterface.FILENAME_PROP);

		// Reading of Shared PROP file
		FileInputStream fIS_shared = new FileInputStream(propFile);
		Properties sharedProperties = new Properties();
		sharedProperties.loadFromXML(fIS_shared);
		fIS_shared.close();

		System.out.println(
				String.format("Properties file <%s> loaded as shared properties.", propFile.getAbsolutePath()));

		int numMaps = Integer.parseInt(
				sharedProperties.getProperty(SimulationInterface.PROP_NAME[SimulationInterface.PROP_NUM_SIM_PER_SET]));

		long[] seedList = new long[numMaps];
		RandomGenerator RNG = new MersenneTwisterRandomGenerator(Long.parseLong(
				sharedProperties.getProperty(SimulationInterface.PROP_NAME[SimulationInterface.PROP_BASESEED])));

		for (int i = 0; i < seedList.length; i++) {
			seedList[i] = RNG.nextLong();
			System.out.println("CMAP_SEED = " + seedList[i]);
		}

		int numThreads = Integer.parseInt(
				sharedProperties.getProperty(SimulationInterface.PROP_NAME[SimulationInterface.PROP_USE_PARALLEL]));

		ExecutorService exec = null;
		if (numThreads > 1) {
			exec = Executors.newFixedThreadPool(numThreads);
		}

		for (int i = 0; i < seedList.length; i++) {
			long seed = seedList[i];
			Runnable_ContactMap_Generation_RMP runnable = new Runnable_ContactMap_Generation_RMP(propFile, seed);
			if (exec != null) {
				exec.submit(runnable);
			} else {
				runnable.run();
			}
		}
		if (exec != null) {
			exec.shutdown();
			try {
				if (!exec.awaitTermination(2, TimeUnit.DAYS)) {
					System.err.println("Thread time-out!");
				}
			} catch (Exception e) {
				e.printStackTrace(System.err);
			}
		}

	}

	private void generate_cMap(File propFile, long seed)
			throws FileNotFoundException, IOException, InvalidPropertiesFormatException {
		// Reading of PROP file
		FileInputStream fIS = new FileInputStream(propFile);
		Properties loadedProperties = new Properties();
		loadedProperties.loadFromXML(fIS);
		fIS.close();
		System.out.println(String.format("Properties file <%s> loaded.", propFile.getAbsolutePath()));

		// Set Location map
		File file_map = new File(loadedProperties
				.getProperty(String.format("%s%d", Simulation_ClusterModelGeneration.POP_PROP_INIT_PREFIX,
						Runnable_Demographic_Generation.RUNNABLE_FIELD_CONTACT_MAP_LOCATION_MAP_PATH)));
		File file_nodeInfo = new File(file_map.getParent(),
				String.format("%s_NoteInfo.csv", file_map.getName().substring(0, file_map.getName().length() - 4)));

		File file_awayPercent = new File(file_map.getParent(),
				String.format("%s_Away.csv", file_map.getName().substring(0, file_map.getName().length() - 4)));

		BufferedReader reader_map = new BufferedReader(new FileReader(file_map));
		BufferedReader reader_nodeinfo = new BufferedReader(new FileReader(file_nodeInfo));
		BufferedReader reader_away = new BufferedReader(new FileReader(file_awayPercent));

		Map_Location_Mobility loc_map = new Map_Location_Mobility();

		loc_map.importNodeInfoFromString(reader_nodeinfo);
		loc_map.importConnectionsFromString(reader_map);
		loc_map.importAwayInfoFromString(reader_away);

		reader_map.close();
		reader_nodeinfo.close();
		reader_away.close();

		boolean genDemo = true;
		boolean genCMap = true;
		boolean genCMapCasual = true;

		loadedProperties.put(Simulation_RMP.PROP_BASEDIR, propFile.getParent());
		loadedProperties.put(Simulation_RMP.PROP_LOC_MAP, loc_map);
		loadedProperties.put(Simulation_RMP.PROP_INDIV_STAT, new ConcurrentHashMap<Integer, int[]>());
		loadedProperties.put(Simulation_RMP.PROP_PARNTER_EXTRA_SOUGHT,
				Collections.synchronizedList(new ArrayList<int[]>()));
		

		if (genDemo) {
			// Generate demographic and mobility
			Runnable_Demographic_Generation run_gen_dem = new Runnable_Demographic_Generation(seed, loadedProperties);
			run_gen_dem.run();
		}

		if (genCMap) {

			Runnable_ContactMap_Generation_Demographic run_gen_cMap = new Runnable_ContactMap_Generation_Demographic(
					seed, loadedProperties);
			run_gen_cMap.run();
		}

		if (genCMapCasual) {
			Runnable_ContactMap_Generation_Hetero_Casual_Partnership_By_Location run_gen_cMap_casual_mobiliy = new Runnable_ContactMap_Generation_Hetero_Casual_Partnership_By_Location(
					seed, loadedProperties);
			run_gen_cMap_casual_mobiliy.run();
		}
	}

}
