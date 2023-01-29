package kabis.benchmark;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

public class Controller {

    public static void waitForStart(){
        try(ServerSocket serverSocket = new ServerSocket(11111)){
            var controller = serverSocket.accept();
            var in = new InputStreamReader(controller.getInputStream());
            in.read();
            in.close();
            controller.close();
        } catch (IOException e) {
            throw new RuntimeException("cannot connect to controller",e);
        }
    }

    public static void main(String[] args) throws IOException {
        System.out.println("Enter a line to start the experiment");
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        br.readLine();
        var sockets = new HashMap<String,Socket>();
        var hosts = args[0].split(",");
        for(var host:hosts){
            sockets.put(host,new Socket(host,11111));
        }
        for(var entry: sockets.entrySet()){
            var socket = entry.getValue();
            try (var out = new ObjectOutputStream(socket.getOutputStream())) {
                out.write(1);
            }
            socket.close();
        }
    }
}
