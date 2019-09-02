import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.reflect.Array;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Logger;

public class CollectTask extends TimerTask {
    FileSystem fs=null;
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH");
    @Override
    public void run() {
        try {
            Logger logger = Logger.getLogger("logRollingFile");

            Properties props=PropertyHolder.getProps();

            String day=sdf.format(new Date());
            File srcDir = new File(props.getProperty(Constants.LOG_SOURCE_DIR));

            File[] listFiles=srcDir.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    if(name.startsWith(props.getProperty(Constants.LOG_LEGAL_PREFIX)))
                    {
                        return true;
                    }
                    return false;
                }
            });

            logger.info("需采集的文件:"+ Arrays.asList(listFiles));

            File uploadFile =new File(props.getProperty(Constants.LOG_TOUPLOAD_DIR));
            for (File file:listFiles)
            {
                FileUtils.moveFileToDirectory(file,uploadFile,true);
            }

            logger.info("采集的文件移动到待上传目录"+uploadFile.getAbsolutePath());


            fs= FileSystem.get(new URI(props.getProperty(Constants.HDFS_URI)),new Configuration(),"root");
            File[] listUploadFiles=uploadFile.listFiles();
            Path path = new Path(props.getProperty(Constants.HDFS_DEST_BASE_DIR)+day);
            if(!fs.exists(path)) {
                fs.mkdirs(path);
            }

            File tempFile =new File(props.getProperty(Constants.LOG_BACKUP_BASE_DIR) + day+"/");

            for (File file : listUploadFiles) {
                Path destPath =new Path(props.getProperty(Constants.HDFS_DEST_BASE_DIR) + day + props.getProperty(Constants.HDFS_FILE_PREFIX) + UUID.randomUUID() + props.getProperty(Constants.HDFS_FILE_SUFFIX));
                fs.copyFromLocalFile(new Path(file.getAbsolutePath().toString()),destPath );
                logger.info("上传目录的文件："+file.getAbsolutePath().toString()+"，传输到hdfs目录完成"+destPath);
                FileUtils.moveFileToDirectory(file,tempFile,true);
                logger.info("文件备份目录："+file.getAbsolutePath().toString()+"--->"+tempFile.getAbsolutePath());
            }
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

    }
}
