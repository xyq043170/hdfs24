import com.sun.xml.bind.v2.runtime.unmarshaller.XsiNilLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;


public class HdfsClientDemo {
    FileSystem fs=null;
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
//       fs.copyFromLocalFile(new Path("D:\\项目相关文档\\测试环境.xlsx"),new Path("/"));
//
//        fs.close();
    }

    @Before
    public void init() throws Exception
    {
        /*
         * 1.configuration先加载jar包默认配置
         * 再加载用户的xx.xml配置
         * 最好是代码中的set
         *
         *
         * */
        Configuration conf = new Configuration();
        //指定客服端上传hdfs上传副本数为2
        conf.set("dfs.replication","2");
        conf.set("dfs.blocksize","64m");
        fs=FileSystem.get(new URI("hdfs://192.168.11.197:9000/"),conf,"root");

    }

    @Test
    public void test() throws IOException {
        fs.copyToLocalFile(new Path("/aaa.txt"),new Path("d:\\"));
        fs.close();
    }

    /*
    *移动+改文件名
    *
    * */

    @Test
    public void testRename() throws Exception{
        fs.rename(new Path("/aaa.txt"),new Path("/aaa/123.txt"));
        fs.close();
    }

    @Test
    public void testMkdir() throws Exception{
        fs.mkdirs(new Path("/xx/yy/zz"));
        fs.close();
    }

    @Test
    public void testRemove() throws Exception{
        fs.delete(new Path("/aaa"),true);
        fs.close();
    }

    @Test
    public void testFind() throws Exception{
        //只查询文件信息，不返回文件夹信息
        RemoteIterator<LocatedFileStatus> LocatedFile= fs.listFiles(new Path("/"),true);
        while (LocatedFile.hasNext())
        {
            LocatedFileStatus status = LocatedFile.next();
            System.out.println(status.getPath());
            System.out.println(status.getBlockSize());
            System.out.println(status.getLen());
            System.out.println(status.getReplication());
            System.out.println(Arrays.asList(status.getBlockLocations()));
            System.out.println(status.getAccessTime());
            System.out.println("-------------------");
        }
        fs.close();
    }

    @Test
    public void testFindFile() throws Exception{
        FileStatus[] fss=fs.listStatus(new Path("/"));

        for (int i = 0;i < fss.length; i++)
        {
            FileStatus status=fss[i];
            if(status.isDirectory())
            {
                System.out.println("is Directory");
            }
            else
            {
                System.out.println("is file");
            }
            System.out.println(status.getPath());
            System.out.println(status.getBlockSize());
            System.out.println(status.getLen());
            System.out.println(status.getReplication());
            System.out.println(status.getAccessTime());
            System.out.println("-------------------");
        }
        fs.close();
    }

    @Test
    public void testReadData() throws Exception{
        FSDataInputStream in =fs.open(new Path("/aaa.txt"));
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        while (br.readLine() != null)
        {
            System.out.println(br.readLine());
        }

        br.close();
        in.close();
        fs.close();

    }

    @Test
    public void testRandomReadData() throws Exception{
        FSDataInputStream in =fs.open(new Path("/bbb.txt"));
        in.seek(8);

        byte[] buf = new byte[16];
        in.read(buf);

        System.out.println(new String(buf));

        in.close();
        fs.close();

    }

    @Test
    public void testWriteData() throws Exception{
        FSDataOutputStream out =fs.create(new Path("/zz.jpg"),false);

        FileInputStream in =new FileInputStream("d:/123.jpg");
        byte[] buf =new byte[1024];
        int reads = 0;
        while ((reads = in.read(buf)) != -1 )
        {
            out.write(buf,0,reads);
        }
        in.close();
        out.close();
        fs.close();

    }
}
