package visualise;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.jgrapht.ext.JGraphXAdapter;
import org.jgrapht.graph.DefaultWeightedEdge;

import com.mxgraph.model.mxICell;
import com.mxgraph.util.mxConstants;

import map.Map_Location_Mobility;

public class Visualise_Map_Location_IREG extends JGraphXAdapter<Integer, DefaultWeightedEdge> {

	private final Map_Location_Mobility map;

	private static final String STYLE_LOC_DEF = "STYLE_LOC_DEF";
	
	
	private static final int[][] grpIndex = new int[][] {
		new int[] {0,1,2,3,4},
		new int[] {5,6,7,8,9},
		new int[] {10,11,12,13,14},
		new int[] {15,16,17,18,19},		
	};
	
	private static final String[] grpName = new String[] {"M_I","F_I","M_N","F_N"};

	public Visualise_Map_Location_IREG(Map_Location_Mobility map_location) {
		super(map_location);
		map = map_location;

		// Style
		setHtmlLabels(true);
		HashMap<String, Object> loc_style = new HashMap<>();
		loc_style.put(mxConstants.STYLE_SHAPE, mxConstants.SHAPE_RECTANGLE);
		loc_style.put(mxConstants.STYLE_FILLCOLOR, "#00FFFF");
		loc_style.put(mxConstants.STYLE_OPACITY, 50);
		loc_style.put(mxConstants.STYLE_FONTCOLOR, "#0000FF");
		loc_style.put(STYLE_LOC_DEF, loc_style);
		getStylesheet().putCellStyle(STYLE_LOC_DEF, loc_style);

		Map<String, Object> edge_style = getStylesheet().getDefaultEdgeStyle();
		edge_style.put(mxConstants.STYLE_FONTSIZE, 0);

		// Vertex adjustment
		String[] labels = map.nodeInfo_header.split(",");
		labels = Arrays.copyOfRange(labels, 2, labels.length); // Skip ID and Name

		HashMap<Integer, mxICell> vMap = getVertexToCellMap();
		for (Entry<Integer, mxICell> vEnt : vMap.entrySet()) {
			getModel().setStyle(vEnt.getValue(), STYLE_LOC_DEF);
			HashMap<String, Object> nodeInfo = map.getNode_info().get(vEnt.getKey());
			if (nodeInfo == null) {
				System.out.printf("Note: Node #%d hidden due to missing info.\n", vEnt.getKey());
				getModel().setVisible(vEnt.getValue(), false);
			} else {
				StringBuilder nodeDisp = new StringBuilder();
				nodeDisp.append("<html>");
				nodeDisp.append("<table border='0'>");
				// Name
				nodeDisp.append("<tr>");
				nodeDisp.append("<td>");
				nodeDisp.append(vEnt.getKey());
				nodeDisp.append("</td>");
				nodeDisp.append("<td>");
				nodeDisp.append(nodeInfo.get(Map_Location_Mobility.NODE_INFO_NAME).toString());
				nodeDisp.append("</td>");
				nodeDisp.append("</tr>");
				// Entry
				int[] popSize = (int[]) nodeInfo.get(Map_Location_Mobility.NODE_INFO_POP_SIZE);
				for (int i = 0; i < grpIndex.length; i++) {
					nodeDisp.append("<tr>");
					nodeDisp.append("<td>");
					nodeDisp.append(grpName[i]);
					nodeDisp.append("</td>");
					nodeDisp.append("<td>");					
					int grpVal = 0;
					for(int j = 0; j < grpIndex[i].length; j++) {
						grpVal += popSize[grpIndex[i][j]];
					}					
					nodeDisp.append(grpVal);
					nodeDisp.append("</td>");
					nodeDisp.append("</tr>");
				}
				
				nodeDisp.append("<tr>");
				for (int i = 0; i < popSize.length; i++) {
					
				}

				
				nodeDisp.append("</table>");
				nodeDisp.append("</html>");
				getModel().setValue(vEnt.getValue(), nodeDisp.toString());
			}

		}

	}
}
