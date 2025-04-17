import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;

import Event.EventSequenceGenerate;
import p2p.P2PHostIdentify;
import p2p.P2PManagementFlowDetect;
import sequence.SequenceDatabase;
import p2p.P2PManagementFlowDetect;
import Event.EventSequenceGenerate;

public class main {
    public static void main(String[] args) {
        try {
            File outputFolder = new File(System.getProperty("user.dir") + "/OutputData");
            if (outputFolder.exists()) { // delete output folder
                File[] files = outputFolder.listFiles();
                for (File file : files) {
//                    file.delete();
                }
            }

//            P2PHostIdentify.run();
////
//////            System.out.println("Start sequence mining");
//            SequenceDatabase.run();
//
//            EventSequenceGenerate.run();

            P2PManagementFlowDetect.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Hello World!");
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

    }
}
