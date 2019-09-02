import java.io.IOException;
import java.util.Properties;

public class PropertyHolder {
    private volatile static Properties prop;

    public static Properties getProps() throws IOException {
        if(prop == null)
        {
            synchronized (PropertyHolder.class)
            {
                if(prop == null)
                {
                    prop = new Properties();
                    prop.load(PropertyHolder.class.getClassLoader().getResourceAsStream("collect.properties"));
                }
            }
        }
        return prop;
    }
}
