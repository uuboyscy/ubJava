package uuboy.scy;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Properties;
import java.util.Vector;
import org.apache.commons.io.IOUtils;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

/**
 * Project: sftpoperation
 * Package: uuboy.scy
 * SFTPOperation.java
 * Description:
 * User: uuboyscy
 * Created Date: 2021/5/14
 * Version: 0.0.1
 */

public class SFTPOperation {

    private ChannelSftp sftp;
    private Session session;
    /* SFTP user */
    private String username;
    /* SFTP password */
    private String password;
    /* Private key for SFTP */
    private String privateKey;
    /* SFTP host */
    private String host;
    /* SFTP port */
    private int port;

    /**
     * Login with password
     * @param username SFTP user
     * @param password SFTP password
     * @param host SFTP host
     * @param port SFTP port
     */
    public SFTPOperation(String username, String password, String host, int port) {
        this.username = username;
        this.password = password;
        this.host = host;
        this.port = port;
    }

    /**
     * Login with private key
     * @param username SFTP user
     * @param host SFTP password
     * @param port SFTP port
     * @param privateKey Private key for SFTP
     */
    public SFTPOperation(String username, String host, int port, String privateKey) {
        this.username = username;
        this.host = host;
        this.port = port;
        this.privateKey = privateKey;
    }

    public SFTPOperation() {}

    public void login() {
        try {
            JSch jsch = new JSch();
            if (privateKey != null) {
                jsch.addIdentity(privateKey);// Set private key
            }
            session = jsch.getSession(username, host, port);
            if (password != null) {
                session.setPassword(password);
            }
            Properties config = new Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);
            session.connect();
            Channel channel = session.openChannel("sftp");
            channel.connect();
            sftp = (ChannelSftp) channel;
        } catch (JSchException e) {
            e.printStackTrace();
        }
    }

    public void logout() {
        if (sftp != null) {
            if (sftp.isConnected()) {
                sftp.disconnect();
            }
        }
        if (session != null) {
            if (session.isConnected()) {
                session.disconnect();
            }
        }
    }

    public void listDir(String path) throws SftpException {
        try {
            Vector<Object> v = sftp.ls(path);
//            System.out.println(v);
            for (Object obj : v) {
                System.out.println(obj);
            }
        } catch (SftpException e) {
            e.printStackTrace();
        }
    }

    /**
     * Send single file from local path, which includes file name, to SFTP
     * @param localPath Local file whole path
     * @param remotePath Remote file whole path
     * */
    public int uploadFile(String localPath, String remotePath) throws FileNotFoundException, SftpException {
        File file = new File(localPath);
        InputStream inputStream = new FileInputStream(file);
        String lastWordRemotePath = remotePath.substring(remotePath.length() - 1);
        String[] localPathArray = localPath.split("/");
        String[] remotePathArray = remotePath.split("/");
        int localPathArrayLength = localPathArray.length;
        int remotePathArrayLength = remotePathArray.length;
        String[] directories = new String[lastWordRemotePath.equals("/") ? remotePathArrayLength : remotePathArrayLength - 1];
        String remoteFileName = lastWordRemotePath.equals("/") ? localPathArray[localPathArrayLength - 1] : remotePathArray[remotePathArrayLength - 1];

        for (int i = 0 ; i < directories.length ; i++) directories[i] = remotePathArray[i];

        // Try to access remote path, create directory if fail
        try {
            sftp.cd(remotePath);
        } catch (SftpException e) {
            // Create directory
            for (String dir : directories) {
                if (dir == null || "".equals(dir)) continue;
                try {
                    sftp.cd(dir);
                } catch (SftpException e2) {
                    sftp.mkdir(dir);
                    sftp.cd(dir);
                }
            }
        }
        sftp.put(inputStream, remoteFileName);
        return 0;
    }

    public boolean uploadFile(String localFolderPath, String remoteFolderPath, String filename, InputStream inputStream) {
        return true;
    }

    /**
     * Upload a folder recursively from local path to SFTP
     * eg. uploadFolder("/tmp/someFolder", "/upload/someRemoteFolder/")
     * This means put the local folder "/tmp/someFolder" into sFTP folder "/upload/someRemoteFolder/",
     * and then get "/upload/someRemoteFolder/someFolder" in sFTP
     * @param localFolderPath Local folder absolute path
     * @param remoteFolderPath Remote folder absolute path
     * */
    public int uploadFolder(String localFolderPath, String remoteFolderPath) {
        String fileName = new File(localFolderPath).getName();
        File[] files = new File(localFolderPath).listFiles();
        int filesLength = files.length;

        try {
            sftp.mkdir(remoteFolderPath + fileName);
        } catch (SftpException e) {
//            e.printStackTrace();
        }

        for (int i = 0 ; i < filesLength ; i++) {
            if (files[i].isDirectory()) {
                uploadFolder(files[i].getAbsolutePath(), remoteFolderPath + fileName + "/");
            } else if (files[i].isFile()) {
                try {
                    uploadFile(files[i].getAbsolutePath(), remoteFolderPath + fileName + "/");
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (SftpException e) {
                    e.printStackTrace();
                }
            }
        }
        return 0;
    }

    public int downloadFile(String remotePath, String localPath) {
        return 0;
    }

    public static void main(String[] args) {
        SFTPOperation sftp = new SFTPOperation("uuboy_sft", "uuboy_sftp", "192.168.50.77", 22);
        sftp.login();
        try {
//            sftp.uploadFile("/tmp/datax", "/upload/");
            sftp.uploadFolder("/tmp/data2", "/upload/");
            sftp.listDir("/upload/file.txt");
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
        } catch (SftpException e) {
            e.printStackTrace();
        }
        sftp.logout();

        System.out.println("===");
//        System.out.println(new File("/tmp/data").isDirectory());
        for (File p : (new File("/tmp/data/20210606").listFiles())) System.out.println(p.getAbsolutePath());
        System.out.println(new File("/tmp/data/20210606").getName());
    }

}
