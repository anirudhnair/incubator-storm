package org.apache.storm;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by anirudhnair on 4/15/16.
 */
public class LBConfigReader {



    private String filePath;
    HashMap<String, HashMap<String,String> > lbConfs;

    LBConfigReader(String sFilePath)
    {
        this.filePath = sFilePath;
        lbConfs = new HashMap<>();
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
            if(nNode.getNodeType() == Node.ELEMENT_NODE) {
            Element eElement = (Element) nNode;
            String lbName = eElement.getAttribute("name");
            lbConfs.put(lbName,new HashMap<String,String>());

            NodeList comps = nNode.getChildNodes();
            // get children and fill in lb config
            for (int child = 0; child < comps.getLength(); ++child) {
                Node nChild = comps.item(child);
                if(nChild.getNodeType() == Node.ELEMENT_NODE) {
                Element eComp = (Element) nChild;
                String name = eComp.getAttribute("name");
                String value = eComp.getAttribute("value");
                lbConfs.get(lbName).put(name,value);
            }}
        }}
        return Common.SUCCESS;
    }

    public String GetValue(String LBType,String name)
    {
        return lbConfs.get(LBType).get(name);
    }



}
