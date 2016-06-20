package org.apache.storm;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.io.IOException;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Element;

/**
 * Created by anirudhnair on 4/15/16.
 */
public class TopoConfigReader {

    class CompConfig
    {
        public String compName;
        public int    instanceNo;
        public int levels;

        CompConfig(String name, int inst, int levels)
        {
            compName = name;
            instanceNo = inst;
            this.levels = levels;
        }
    }

    private String filePath;
    HashMap<String, ArrayList<CompConfig> > CompConfs;

    TopoConfigReader(String sFilePath)
    {
        this.filePath = filePath;
        CompConfs = new HashMap<>();
    }

    public int Init()
    {
        File fXmlFile = new File(filePath);
        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder;
        try {
            dBuilder = dbFactory.newDocumentBuilder();
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
            return Common.FAILURE;
        }
        Document doc;
        try {
            doc = dBuilder.parse(fXmlFile);
        } catch (SAXException e) {
            e.printStackTrace();
            return Common.FAILURE;
        } catch (IOException e) {
            e.printStackTrace();
            return Common.FAILURE;
        }

        Node root = doc.getDocumentElement();
        NodeList nList = root.getChildNodes();
        for (int temp = 0; temp < nList.getLength(); temp++) {
            Node nNode = nList.item(temp);
            // get the name
            Element eElement = (Element) nNode;
            String topoName = eElement.getAttribute("name");
            CompConfs.put(topoName,new ArrayList<CompConfig>());

            NodeList comps = nNode.getChildNodes();
            // get children and fill in component config
            for (int child = 0; child < comps.getLength(); ++child) {
                Node nChild = comps.item(child);
                Element eComp = (Element) nChild;
                String compName = eComp.getAttribute("name");
                int instance = Integer.parseInt(eComp.getAttribute("instances"));
                String sLevels = eComp.getAttribute("levels");
                int levels = 0;
                if(!"".equals(sLevels)) {
                    levels = Integer.parseInt(sLevels);
                }
                CompConfig compConf = new CompConfig(compName,instance,levels);
                CompConfs.get(topoName).add(compConf);
            }
        }
        return Common.SUCCESS;
    }

    public int GetInstance(String topoName,String compName)
    {
        ArrayList<CompConfig> comps = CompConfs.get(topoName);
        for ( CompConfig conf: comps )
        {
            if(conf.compName.equals(compName))
            {
                return conf.instanceNo;
            }
        }
        return 0;
    }

    public int GetLevels(String topoName,String compName)
    {
        ArrayList<CompConfig> comps = CompConfs.get(topoName);
        for ( CompConfig conf: comps )
        {
            if(conf.compName.equals(compName))
            {
                return conf.levels;
            }
        }
        return 0;
    }

}
