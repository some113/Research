import java.io.BufferedReader;
import java.io.InputStreamReader;

import p2p.P2PHostIdentify;
import p2p.P2PManagementFlowDetect;
import sequence.SequenceDatabase;
import p2p.P2PManagementFlowDetect;

public class main {
    public static void main(String[] args) {
        try {
//            P2PHostIdentify.run();


            long end = System.currentTimeMillis() + 15 * 1000;

//            Thread.sleep(30 * 1000);
//            System.out.println("Start sequence mining");
//            SequenceDatabase.run();

            P2PManagementFlowDetect.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Hello World!");
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

    }
}
