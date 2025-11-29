package sim;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.InvalidPropertiesFormatException;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import map.Map_Location_Mobility;
import random.MersenneTwisterRandomGenerator;
import random.RandomGenerator;

public class Runnable_RMP_Map_Generation implements Runnable {

	File propFile;
	long seed;
	int gen_setting;

	private int GEN_SETTING_DEMOGRAPHIC = 1 << 0;
	private int GEN_SETTING_CONTACT_MAP = 1 << 1;
	private int GEN_SETTING_CASUAL_MAP = 1 << 2;
	
	public Runnable_RMP_Map_Generation(File propFile, long cmap_seed, int gen_setting) {
		this.propFile = propFile;
		this.seed = cmap_seed;
		this.gen_setting = gen_setting;
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
		final String USAGE_INFO = String.format(
				"Usage: java %s PROP_FILE_DIRECTORY GEN_SETTING <-seedList=SEEDLIST> <-genSeed=BASESEED,NUM_SIM>"
						+ "\n",
				Runnable_RMP_Map_Generation.class.getName());

		if (args.length < 2) {
			System.out.println(USAGE_INFO);
			System.exit(0);
		}

		File baseDir = new File(args[0]);
		File propFile = new File(baseDir, SimulationInterface.FILENAME_PROP);

		int gen_setting = Integer.parseInt(args[1]);

		long baseSeed = 0;
		int numMaps = 0;

		// Check if there is a user input seed list
		long[] seedList = null;
		for (String arg : args) {
			if (arg.startsWith("-seedList=")) {
				String[] seed_list_str = arg.substring("-seedList=".length()).split(",");
				seedList = new long[seed_list_str.length];
				for (int i = 0; i < seed_list_str.length; i++) {
					seedList[i] = Long.parseLong(seed_list_str[i]);
				}
			}
			if (arg.startsWith("-genSeed=")) {
				String[] gen_seed_str = arg.substring("-genSeed=".length()).split(",");
				baseSeed = Long.parseLong(gen_seed_str[0]);
				numMaps = Integer.parseInt(gen_seed_str[1]);
			}

		}

		// Reading of Shared PROP file
		FileInputStream fIS_shared = new FileInputStream(propFile);
		Properties sharedProperties = new Properties();
		sharedProperties.loadFromXML(fIS_shared);
		fIS_shared.close();

		if (seedList == null) {
			if (numMaps <= 0) {
				numMaps = Integer.parseInt(sharedProperties
						.getProperty(SimulationInterface.PROP_NAME[SimulationInterface.PROP_NUM_SIM_PER_SET]));
			}
			if (baseSeed <= 0) {
				baseSeed = Long.parseLong(
						sharedProperties.getProperty(SimulationInterface.PROP_NAME[SimulationInterface.PROP_BASESEED]));
			}

			seedList = new long[numMaps];
			RandomGenerator RNG = new MersenneTwisterRandomGenerator(baseSeed);
			for (int i = 0; i < seedList.length; i++) {
				seedList[i] = RNG.nextLong();
				System.out.println("Generated CMAP_SEED = " + seedList[i]);
			}
		}

		int numThreads = seedList.length;

		if (sharedProperties.containsKey(SimulationInterface.PROP_NAME[SimulationInterface.PROP_USE_PARALLEL])) {
			numThreads = Math.min(
					Integer.parseInt(sharedProperties
							.getProperty(SimulationInterface.PROP_NAME[SimulationInterface.PROP_USE_PARALLEL])),
					seedList.length);
		}

		ExecutorService exec = null;
		if (numThreads > 1) {
			exec = Executors.newFixedThreadPool(numThreads);
		}

		for (int i = 0; i < seedList.length; i++) {
			long seed = seedList[i];
			Runnable_RMP_Map_Generation runnable = new Runnable_RMP_Map_Generation(propFile, seed, gen_setting);
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
		Properties loadedPropertiesProp = new Properties();
		loadedPropertiesProp.loadFromXML(fIS);
		fIS.close();
		System.out.println(String.format("Properties file <%s> loaded.", propFile.getAbsolutePath()));

		HashMap<String, Object> loadedProperties = new HashMap<>();
		for (Entry<Object, Object> ent : loadedPropertiesProp.entrySet()) {
			loadedProperties.put(String.valueOf(ent.getKey()), String.valueOf(ent.getValue()));
		}

		// Set Location map
		File file_map = new File((String) loadedProperties
				.get(String.format("%s%d", Simulation_ClusterModelGeneration.POP_PROP_INIT_PREFIX,
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

		boolean genDemo = (gen_setting & GEN_SETTING_DEMOGRAPHIC) != 0;
		boolean genCMap = (gen_setting & GEN_SETTING_CONTACT_MAP) != 0;
		boolean genCMapCasual = (gen_setting & GEN_SETTING_CASUAL_MAP) != 0;

		loadedProperties.put(Simulation_Gen_MetaPop.PROP_BASEDIR, propFile.getParent());
		loadedProperties.put(Simulation_Gen_MetaPop.PROP_LOC_MAP, loc_map);		

		if (genDemo) {
			// Generate demographic and mobility
			Runnable_Demographic_Generation run_gen_dem = new Runnable_Demographic_Generation(seed, loadedProperties);
			run_gen_dem.run();
		}

		if (genCMap) {
			Runnable_ContactMap_Generation run_gen_cMap = new Runnable_ContactMap_Generation(
					seed, loadedProperties);
			run_gen_cMap.run();
		}

		if (genCMapCasual) {
			Runnable_Hetero_Casual_Partnership_Generation run_gen_cMap_casual_mobiliy = new Runnable_Hetero_Casual_Partnership_Generation(
					seed, loadedProperties);
			run_gen_cMap_casual_mobiliy.run();
		}
	}

}
