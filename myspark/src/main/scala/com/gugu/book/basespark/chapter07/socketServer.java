package com.gugu.book.basespark.chapter07;

import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class socketServer {
    public static void main(String[] args) throws IOException, InterruptedException {
        ServerSocket serverSocket = new ServerSocket(9999);
        Socket socket = serverSocket.accept();
        OutputStream outputStream = socket.getOutputStream();
        for (int i = 0; i < 100; i++) {
            outputStream.write("hello gugu".getBytes());
            outputStream.write("hello world".getBytes());
            outputStream.flush();
        }
        outputStream.close();
        socket.close();
    }
}
