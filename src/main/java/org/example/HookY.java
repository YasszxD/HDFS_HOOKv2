package org.example;
/*****/

import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.hook.AtlasHook;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.notification.HookNotification;
import org.apache.atlas.model.notification.HookNotification.EntityCreateRequestV2;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.v1.model.instance.Referenceable;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSInotifyEventInputStream;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.hadoop.hdfs.inotify.MissingEventsException;
import org.apache.logging.log4j.Logger;
import org.example.HdfsY;
import org.jline.utils.Log;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;




public class HookY extends AtlasHook {
    static FileSystem fileSystem = null;
    static Configuration config = null;
    public static String TypeName="csv_files";

    private static final AtlasClient atlasClient = new AtlasClient(new String[]{"http://hadoopnamenode:21000"}, new String[]{"admin", "admin"});

    public static int anInt;

    public static void main(String[] args) throws IOException, MissingEventsException, InterruptedException, AtlasServiceException, ParserConfigurationException, SAXException {

        //atlasClient = new AtlasClient(new String[]{"http://hadoopnamenode:21000"}, new String[]{"admin", "admin"});

        HdfsY hdfsY = new HdfsY();
        config = HdfsY.config;
        fileSystem = FileSystem.get(config);
        anInt=3;

        //createprocess("RenameProcess","24731f2a-506a-457d-a8b4-5bec0eff2ee4","9fc8c073-33d5-4040-b90c-22514ae2dd4e");
        //aVoid();
        // Start the hook
        notif(TypeName);
    }



/*  public static void aVoid(){
        List<HookNotification> messages = new ArrayList<>();
        AtlasEntity cre2 = new AtlasEntity();
        cre2.setTypeName("renameProcess");

        List<AtlasObjectId> objectIds1 = new ArrayList<>();

        List<AtlasObjectId> objectIds = new ArrayList<>();


        cre2.setAttribute("qualifiedName","p1@renameProcess");
        cre2.setAttribute("name","p1");
        //cre2.setAttribute("inputs",);
        AtlasEntity cre0 = new AtlasEntity();
        AtlasEntity cre1 = new AtlasEntity();
        AtlasObjectId atlasObjectId = new AtlasObjectId();
        atlasObjectId.setGuid("c9e9201c-0fe2-4e86-b3f7-84fb1deb12c3");
        atlasObjectId.setTypeName("csv_files");
        AtlasObjectId atlasObjectId1 = new AtlasObjectId();
        atlasObjectId1.setGuid("b430e1fc-9a47-46b5-a101-31b8f5891de7");
        atlasObjectId1.setTypeName("csv_files");
        objectIds.add(atlasObjectId);
        objectIds1.add(atlasObjectId1);
        cre2.setAttribute("inputs",objectIds1);
        cre2.setAttribute("outputs",objectIds);

        messages.add(new EntityCreateRequestV2("yexz", new AtlasEntity.AtlasEntitiesWithExtInfo(cre2)));
        notifyEntities(messages, null,5,null);
    }*/


    public static void notif(String TypeName) throws IOException, MissingEventsException, InterruptedException, AtlasServiceException, ParserConfigurationException, SAXException {
        HdfsAdmin admin = new HdfsAdmin(fileSystem.getUri(), config);
        DFSInotifyEventInputStream eventStream = admin.getInotifyEventStream();

        while (true) {
            EventBatch events = eventStream.take();
            for (Event event : events.getEvents()) {
                System.out.println("event type = " + event.getEventType());

                switch (event.getEventType()) {
                    case CREATE:
                        Event.CreateEvent createEvent = (Event.CreateEvent) event;
                        if (createEvent.getPath().startsWith("/tmp/hadoop-yarn/staging/yexz/.staging/") && createEvent.getPath().endsWith("job.xml")) {
                            System.out.println(createEvent.getPath());
                            String command = "./home/yexz/hadoop-3.3.4/bin/hdfs dfs -copyToLocal "+ createEvent.getPath() + "/home/yexz/";
                            try {
                                Process process = Runtime.getRuntime().exec(command);
                                process.waitFor(); // wait for the command to finish
                            } catch (IOException | InterruptedException e) {
                                e.printStackTrace(); // handle any exceptions
                            }

                            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                            DocumentBuilder builder = factory.newDocumentBuilder();

                            //Document document = builder.parse(HdfsY.readFileFromHDFS(createEvent.getPath()));
                            Document document = builder.parse("/home/yexz/job.xml");

                            NodeList records = document.getElementsByTagName("property");
                            String ouput = null;
                            String input = null;
                            String processname = null;
                            for (int i = 0; i < records.getLength(); i++) {
                                Element record = (Element) records.item(i);
                                NodeList opcode = record.getElementsByTagName("name");
                                if (opcode.item(0).getTextContent().equals("mapreduce.input.fileinputformat.inputdir"))
                                    input = record.getElementsByTagName("value").item(0).getTextContent();
                                if (opcode.item(0).getTextContent().equals("mapreduce.output.fileoutputformat.outputdir"))
                                    ouput= record.getElementsByTagName("value").item(0).getTextContent();
                                if (opcode.item(0).getTextContent().equals("mapreduce.job.name"))
                                    processname=record.getElementsByTagName("value").item(0).getTextContent();

                            }
                            createProcess(processname,getReferancableByName(input),getReferancableByName(ouput));
                        }

                        if (!createEvent.getPath().startsWith("/user/yexz/"))
                            break;


                        System.out.println("start creating entity...");
                        createEntity(createEvent.getPath().replace("._COPYING_",""),false);
                        System.out.println("created");
                        break;
                    case RENAME:
                        Event.RenameEvent renameEvent = (Event.RenameEvent) event;
                        if (!renameEvent.getSrcPath().startsWith("/user/yexz/") || renameEvent.getSrcPath().endsWith("._COPYING_"))
                            break;
                        anInt++;
                        //create entity
                        System.out.println("start creating entity...");
                        createEntity(renameEvent.getDstPath(),false);
                        System.out.println("created entity");

                        try {
                            Thread.sleep(2000); // Sleep for 1000 milliseconds (1 second)
                        } catch (InterruptedException e) {
                            // Handle any exceptions that may occur during sleep
                            e.printStackTrace();
                        }

                        //get old entity
                        Log.info("file src +++++++++"+renameEvent.getSrcPath());
                        String qualifiedName = renameEvent.getSrcPath().split("/")[3]+"@csv_files";
                        Referenceable oldEntity = atlasClient.getEntity("csv_files","qualifiedName",qualifiedName);
                        //get new entity
                        Log.info("file dst +++++++++"+renameEvent.getDstPath());
                        String qualifiedNameNew = renameEvent.getDstPath().split("/")[3]+"@csv_files";
                        Referenceable NewEntity = atlasClient.getEntity("csv_files","qualifiedName",qualifiedNameNew);
                        //create Process entity
                        Log.info("+++++++++"+oldEntity.getId());
                        Log.info("+++++++++"+NewEntity.getId());
                        System.out.println("start creating process...");
                        createProcess("renameProcess", oldEntity, NewEntity);
                        System.out.println("created process");
                        break;


                    case UNLINK:
                        Event.UnlinkEvent unlinkEvent = (Event.UnlinkEvent) event;
                        System.out.println("  path = " + unlinkEvent.getPath());
                        break;

                    case APPEND:
                        Event.AppendEvent appendEvent = (Event.AppendEvent) event;
                        System.out.println("  path = " + appendEvent.getPath());

                    case METADATA:
                        assert event instanceof Event.MetadataUpdateEvent;
                        Event.MetadataUpdateEvent metadataUpdateEvent = (Event.MetadataUpdateEvent) event;
                        //System.out.println("  path = " + metadataUpdateEvent.getPath());

                    default:
                        break;
                }
            }
        }


    }


    public static void createProcess(String typeName, Referenceable oldEntity, Referenceable newEntity){
        List<HookNotification> messages = new ArrayList<>();

        AtlasEntity cre2 = new AtlasEntity();
        cre2.setTypeName("renameProcess");
        String name = "renameProcess"+anInt;
        String qualifiedName= name+"@renameProcess";
        cre2.setAttribute("qualifiedName",qualifiedName);
        cre2.setAttribute("name",name);

        List<AtlasObjectId> oldArray = new ArrayList<>();
        AtlasObjectId oldAtlasObjectId = new AtlasObjectId();
        oldAtlasObjectId.setGuid(oldEntity.getId().getId());
        oldArray.add(oldAtlasObjectId);

        List<AtlasObjectId> newArray = new ArrayList<>();
        AtlasObjectId newAtlasObjectId = new AtlasObjectId();
        newAtlasObjectId.setGuid(newEntity.getId().getId());
        newArray.add(newAtlasObjectId);

        cre2.setAttribute("inputs",oldArray);
        cre2.setAttribute("outputs",newArray);

        messages.add(new EntityCreateRequestV2("yexz", new AtlasEntity.AtlasEntitiesWithExtInfo(cre2)));
        notifyEntities(messages, null,10,null);

    }
    public static void createEntity(String filePath,boolean fromHdfs) throws IOException {
        List<HookNotification> messages = new ArrayList<>();

        AtlasEntity cre = new AtlasEntity();
        cre.setTypeName(TypeName);
        Path path = new Path(filePath);
        cre.setAttribute("owner",fileSystem.getFileStatus(path).getOwner());
        cre.setAttribute("path",fileSystem.getFileStatus(path).getPath());
        cre.setAttribute("ModificationTime",fileSystem.getFileStatus(path).getModificationTime());
        System.out.println("********here");
        System.out.println(fromHdfs);
        if(fromHdfs) {
            Map<String, byte[]> attrs = HdfsY.getAttr(filePath);
            //metadata from hadoop attrs
            for (Map.Entry<String, byte[]> entry : attrs.entrySet()) {
                String xAttrName = entry.getKey().substring(5);
                byte[] xAttrValue = entry.getValue();
                String encodedValue = new String(xAttrValue, StandardCharsets.UTF_8);
                cre.setAttribute(xAttrName, encodedValue);
                System.out.println(xAttrName + "  " + encodedValue);

            }
            System.out.println("thats true");
        }
        else {
            System.out.println("****************************");
            cre.setAttribute("name",filePath.split("/")[3]);
            System.out.println(filePath.split("/")[3]+"@csv_files");
            cre.setAttribute("displayName",filePath.split("/")[3]);
            cre.setAttribute("qualifiedName",filePath.split("/")[3]+"@csv_files");
        }
        System.out.println("ending... ");
        messages.add(new EntityCreateRequestV2("yexz", new AtlasEntity.AtlasEntitiesWithExtInfo(cre)));
        notifyEntities(messages, null,10,null);

    }


    public void concatenate(String file1, String file2, String file3){

    }
    public static Referenceable getReferancableByName(String name) throws AtlasServiceException {
        String qualifiedName = name+"@csv_files";
        return atlasClient.getEntity("csv_files","qualifiedName",qualifiedName);
    }



    @Override
    public String getMessageSource() {
        return null;
    }


}