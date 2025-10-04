package sim;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import map.Map_Location_Mobility;
import person.AbstractIndividualInterface;
import random.MersenneTwisterRandomGenerator;
import random.RandomGenerator;
import util.LineCollectionEntry;

public class Runnable_ContactMap_Generation_Hetero_Casual_Partnership_By_Location implements Runnable {

	protected boolean preloadFile = false;
	protected long mapSeed;
	protected Properties loadedProperties;

	protected HashMap<String, LineCollectionEntry> demogrpahicCollections = new HashMap<>();
	protected HashMap<String, LineCollectionEntry> movementCollections = new HashMap<>();
	protected HashMap<String, LineCollectionEntry> extraPartnerCollections = new HashMap<>();

	protected ArrayList<int[]> extra_partnership_formed = new ArrayList<>();
	private RandomGenerator RNG;
	private int NUM_AGE_GRP; // So Grp number / NUM_AGE_GRP is gender

	public Runnable_ContactMap_Generation_Hetero_Casual_Partnership_By_Location(long mapSeed,
			Properties loadedProperties) {
		this.mapSeed = mapSeed;
		this.loadedProperties = loadedProperties;
		this.RNG = new MersenneTwisterRandomGenerator(mapSeed);
		// Format: POP_ID,Name,M0,M1... F0,F1...
		this.NUM_AGE_GRP = (((Map_Location_Mobility) loadedProperties.get(Simulation_RMP.PROP_LOC_MAP))
				.getNodeInfo_header().split(",").length - 2)/2;

	}

	protected Properties getLoadedProperties() {
		return loadedProperties;
	}

	protected long getMapSeed() {
		return mapSeed;
	}

	public void run() {

		long tic = System.currentTimeMillis();
		File baseDir = (File) getLoadedProperties().get(Simulation_RMP.PROP_BASEDIR);

		// Load demographic file
		loadCollection(baseDir,
				Pattern.compile(Runnable_Demographic_Generation.FILENAME_FORMAT_DEMOGRAPHIC
						.replaceFirst("%d", "(\\\\d+)").replaceFirst("%d", Long.toString(getMapSeed()))),
				demogrpahicCollections);

		// Load movement file
		loadCollection(baseDir,
				Pattern.compile(Runnable_Demographic_Generation.FILENAME_FORMAT_MOVEMENT
						.replaceFirst("%s", "(\\\\d+_\\\\d+)").replaceAll("%d", Long.toString(getMapSeed()))),
				movementCollections);
		// Load extra partners
		loadCollection(baseDir,
				Pattern.compile(String.format(
						Runnable_ContactMap_Generation_Demographic.FILENAME_FORMAT_EXTRA_PARTNER_SOUGHT, getMapSeed())),
				extraPartnerCollections);

		int currentTime = 0;
		int max_time = Integer.parseInt(
				getLoadedProperties().getProperty(SimulationInterface.PROP_NAME[SimulationInterface.PROP_NUM_SNAP]))
				* Integer.parseInt(getLoadedProperties()
						.getProperty(SimulationInterface.PROP_NAME[SimulationInterface.PROP_SNAP_FREQ]));

		// Key = PID, Val = See Runnable_MetaPopulation_Transmission_RMP_MultiInfection.
		HashMap<Integer, int[]> indiv_map = new HashMap<>();
		// Key = PID, Val = int[]{number_to_seek, valid_until}
		HashMap<Integer, int[]> seek_extra_partners = new HashMap<>();
		HashMap<Integer, HashMap<Integer, ArrayList<Integer>>> seek_extra_by_gender_loc = new HashMap<>();

		while (currentTime < max_time) {
			updateIndivduals(currentTime, indiv_map, seek_extra_partners, seek_extra_by_gender_loc);

			// Update movement
			for (Entry<String, LineCollectionEntry> mvE : movementCollections.entrySet()) {
				String[] direction = mvE.getKey().split("_");
				while ((mvE.getValue().getCurrentLine()) != null) {
					String[] ent = mvE.getValue().getCurrentLine().split(",");
					if (Integer.parseInt(ent[0]) == currentTime) {
						Integer pid = Integer.parseInt(ent[1]);
						int[] indiv_stat = indiv_map.get(pid);
						int src_loc = Integer.parseInt(direction[0]);
						int tar_loc = Integer.parseInt(direction[1]);

						if (src_loc != indiv_stat[Runnable_MetaPopulation_Transmission_RMP_MultiInfection.INDIV_MAP_CURRENT_LOC]) {
							System.err.printf("Warning! Movement mismatch\n");
						}

						indiv_stat[Runnable_MetaPopulation_Transmission_RMP_MultiInfection.INDIV_MAP_CURRENT_LOC] = tar_loc;
						int gender = indiv_stat[Runnable_MetaPopulation_Transmission_RMP_MultiInfection.INDIV_MAP_ENTER_GRP]
								/ NUM_AGE_GRP;

						int[] extra_stat = seek_extra_partners.get(pid);
						if (extra_stat != null && extra_stat[0] > 0) {
							HashMap<Integer, ArrayList<Integer>> seek_extra_by_loc = seek_extra_by_gender_loc
									.get(gender);
							if (seek_extra_by_loc == null) {
								seek_extra_by_loc = new HashMap<>();
								seek_extra_by_gender_loc.put(gender, seek_extra_by_loc);
							}

							seek_extra_by_loc.get(src_loc)
									.remove(Collections.binarySearch(seek_extra_by_loc.get(src_loc), pid));

							seek_extra_by_loc.get(tar_loc)
									.add(~Collections.binarySearch(seek_extra_by_loc.get(tar_loc), pid), pid);
						}

						mvE.getValue().loadNextLine();
					} else {
						break;
					}
				}
			}

			// Update seek_extra_partners and form casual partnerships
			Integer[] seek_extra_partner_seekers = seek_extra_partners.keySet().toArray(new Integer[0]);
			HashMap<Integer, ArrayList<Integer>> seek_extra_by_loc;
			Arrays.sort(seek_extra_partner_seekers);

			for (Integer pid_seeker : seek_extra_partner_seekers) {
				int[] extra_sought_ent_seeker = seek_extra_partners.get(pid_seeker);
				if (extra_sought_ent_seeker != null) {
					int[] indiv_stat_seeker = indiv_map.get(pid_seeker);
					int gender_seeker = indiv_stat_seeker[Runnable_MetaPopulation_Transmission_RMP_MultiInfection.INDIV_MAP_ENTER_GRP]
							/ NUM_AGE_GRP;
					int common_loc = indiv_stat_seeker[Runnable_MetaPopulation_Transmission_RMP_MultiInfection.INDIV_MAP_CURRENT_LOC];
					if (indiv_stat_seeker[Runnable_MetaPopulation_Transmission_RMP_MultiInfection.INDIV_MAP_EXIT_POP_AT] != -1
							&& indiv_stat_seeker[Runnable_MetaPopulation_Transmission_RMP_MultiInfection.INDIV_MAP_EXIT_POP_AT] < currentTime) {
						// Remove expired
						seek_extra_partners.remove(pid_seeker);
						seek_extra_by_loc = seek_extra_by_gender_loc.get(gender_seeker);
						seek_extra_by_loc.get(common_loc)
								.remove(Collections.binarySearch(seek_extra_by_loc.get(common_loc), pid_seeker));
					} else {
						// Check if seeking partners today
						if (RNG.nextInt(
								Math.max(extra_sought_ent_seeker[1] - currentTime, 1)) < extra_sought_ent_seeker[0]) {
							ArrayList<Integer> seeker_partners_by_loc = seek_extra_by_gender_loc.get(1 - gender_seeker)
									.get(common_loc);

							if (seeker_partners_by_loc.size() > 1) {
								int index = RNG.nextInt(seeker_partners_by_loc.size());
								Integer partner_pid = seeker_partners_by_loc.get(index);
								extra_partnership_formed.add(new int[] { pid_seeker, partner_pid, currentTime, 1 });

								for (int pid_r : new int[] { pid_seeker, partner_pid }) {
									seek_extra_partners.get(pid_r)[0]--;
									if (seek_extra_partners.get(pid_r)[0] == 0) {
										int gender_r = indiv_map.get(
												pid_r)[Runnable_MetaPopulation_Transmission_RMP_MultiInfection.INDIV_MAP_ENTER_GRP]
												/ NUM_AGE_GRP;
										seek_extra_partners.remove(pid_r);
										seek_extra_by_loc = seek_extra_by_gender_loc.get(gender_r);
										seek_extra_by_loc.get(common_loc).remove(
												Collections.binarySearch(seek_extra_by_loc.get(common_loc), pid_r));

									}

								}
							}

						}
					}

				}

			}

			currentTime++;
		}

		// Extra partners sought
		PrintWriter pWri;
		try {
			pWri = new PrintWriter(new File(baseDir, String
					.format(Runnable_ClusterModel_ContactMap_Generation_MultiMap.MAPFILE_FORMAT, 0, getMapSeed())));

			for (int[] extra_p : extra_partnership_formed) {
				pWri.printf("%d,%d,%d,%d\n", extra_p[0], extra_p[1], extra_p[2], extra_p[3]);
			}

			pWri.close();
		} catch (IOException e) {
			e.printStackTrace(System.err);
		}

		System.out.printf("Casual contact map generation for cMap=%d completed. Time required = %.3fs\n", mapSeed,
				(System.currentTimeMillis() - tic) / 1000.0);
	}

	protected void updateIndivduals(int currentTime, HashMap<Integer, int[]> indiv_map,
			HashMap<Integer, int[]> seek_extra_partners,
			HashMap<Integer, HashMap<Integer, ArrayList<Integer>>> seek_extra_by_gender_loc) {
		updateIndivdualMap(currentTime, indiv_map);

		for (LineCollectionEntry extraPartnerCollections : extraPartnerCollections.values()) {
			String currentEnt = extraPartnerCollections.getCurrentLine();
			// TIME_FROM,PID,EXTRA_PARTNER_SOUGHT
			String[] ent_sp = currentEnt == null ? null : currentEnt.split(",");
			int time_from = ent_sp == null ? Integer.MAX_VALUE : Integer.parseInt(ent_sp[0]);
			while (time_from <= currentTime) {
				int pid = Integer.parseInt(ent_sp[1]);
				int gender = indiv_map
						.get(pid)[Runnable_MetaPopulation_Transmission_RMP_MultiInfection.INDIV_MAP_ENTER_GRP]
						/ NUM_AGE_GRP;

				int partner_sought = Integer.parseInt(ent_sp[2]);
				int time_until = time_from + AbstractIndividualInterface.ONE_YEAR_INT;
				int[] extra_sought_ent = seek_extra_partners.get(pid);
				if (extra_sought_ent == null) {
					extra_sought_ent = new int[2]; // int[]{number_to_seek, valid_until}
					seek_extra_partners.put(pid, extra_sought_ent);

					HashMap<Integer, ArrayList<Integer>> seek_extra_by_loc = seek_extra_by_gender_loc.get(gender);
					if (seek_extra_by_loc == null) {
						seek_extra_by_loc = new HashMap<>();
						seek_extra_by_gender_loc.put(gender, seek_extra_by_loc);
					}

					int curLoc = indiv_map
							.get(pid)[Runnable_MetaPopulation_Transmission_RMP_MultiInfection.INDIV_MAP_CURRENT_LOC];
					ArrayList<Integer> seek_extra_pids = seek_extra_by_loc.get(curLoc);
					if (seek_extra_pids == null) {
						seek_extra_pids = new ArrayList<>();
						seek_extra_by_loc.put(curLoc, seek_extra_pids);
					}
					seek_extra_pids.add(~Collections.binarySearch(seek_extra_pids, pid), pid);

				}
				extra_sought_ent[0] += partner_sought;
				extra_sought_ent[1] = time_until;

				extraPartnerCollections.loadNextLine();
				currentEnt = extraPartnerCollections.getCurrentLine();
				ent_sp = currentEnt == null ? null : currentEnt.split(",");
				time_from = ent_sp == null ? Integer.MAX_VALUE : Integer.parseInt(ent_sp[0]);

			}
		}

	}

	protected void updateIndivdualMap(int currentTime, HashMap<Integer, int[]> indiv_map) {
		for (Entry<String, LineCollectionEntry> ent : demogrpahicCollections.entrySet()) {
			int home_loc = Integer.parseInt(ent.getKey());
			LineCollectionEntry lines_demographic = ent.getValue();
			String line_demo = lines_demographic.getCurrentLine();
			// 0:PID,1:ENTER_AT,2:EXIT_AT,3:ENTER_AGE,4:ENTER_GRP
			String[] str_ent = line_demo == null ? null : line_demo.split(",");
			while (str_ent != null) {
				int pid = Integer.parseInt(str_ent[0]);
				int enter_at = Integer.parseInt(str_ent[1]);

				if (enter_at > currentTime) {
					break;
				}

				int[] indiv_ent = new int[Runnable_MetaPopulation_Transmission_RMP_MultiInfection.LENGTH_INDIV_MAP];
				indiv_ent[Runnable_MetaPopulation_Transmission_RMP_MultiInfection.INDIV_MAP_ENTER_POP_AT] = enter_at;
				indiv_ent[Runnable_MetaPopulation_Transmission_RMP_MultiInfection.INDIV_MAP_EXIT_POP_AT] = Integer
						.parseInt(str_ent[2]);
				indiv_ent[Runnable_MetaPopulation_Transmission_RMP_MultiInfection.INDIV_MAP_ENTER_POP_AGE] = Integer
						.parseInt(str_ent[3]);
				indiv_ent[Runnable_MetaPopulation_Transmission_RMP_MultiInfection.INDIV_MAP_CURRENT_GRP] = Integer
						.parseInt(str_ent[4]);

				indiv_ent[Runnable_MetaPopulation_Transmission_RMP_MultiInfection.INDIV_MAP_HOME_LOC] = home_loc;
				indiv_ent[Runnable_MetaPopulation_Transmission_RMP_MultiInfection.INDIV_MAP_ENTER_GRP] = Integer
						.parseInt(str_ent[4]);
				indiv_ent[Runnable_MetaPopulation_Transmission_RMP_MultiInfection.INDIV_MAP_CURRENT_LOC] = home_loc;
				indiv_map.put(pid, indiv_ent);

				lines_demographic.loadNextLine();
				line_demo = lines_demographic.getCurrentLine();
				str_ent = line_demo == null ? null : line_demo.split(",");
			}
		}
	}

	protected void loadCollection(File baseDir, Pattern csv_pattern, HashMap<String, LineCollectionEntry> collection) {
		File[] csv_files = baseDir.listFiles(new FileFilter() {
			@Override
			public boolean accept(File pathname) {
				return csv_pattern.matcher(pathname.getName()).matches();
			}
		});
		for (File csv : csv_files) {
			try {
				Matcher m = csv_pattern.matcher(csv.getName());
				m.matches();
				LineCollectionEntry ent = new LineCollectionEntry(csv, preloadFile);
				ent.loadNextLine();
				ent.loadNextLine(); // Skip Header
				if (m.groupCount() >= 1) {
					collection.put(m.group(1), ent);
				} else {
					collection.put(csv_pattern.toString(), ent);
				}
			} catch (IOException e) {
				e.printStackTrace(System.err);
			}
		}
	}

}
