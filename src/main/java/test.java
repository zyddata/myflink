import com.sun.org.apache.xml.internal.security.keys.content.X509Data;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author zhangyide
 * @desc
 * @date 2019/7/517:12
 */
public class test {
	public static void main(String[] args) {

		List list = new ArrayList();
		List list2 = new ArrayList();
		for (int i = 0; i < 40000000; i++) {
			list.add(i);
		}
		Long start = System.currentTimeMillis();
		Object collect = list.parallelStream().collect(Collectors.toSet());
		Long end = System.currentTimeMillis();
		System.out.println(end - start); //26098
	}
}
