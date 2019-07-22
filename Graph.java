
import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

class Vertex implements Writable 
{
    public short tag;                                   // 0 for a graph vertex, 1 for a group number
    public long group;                                  // the group where this vertex belongs to
    public long VID;                                    // the vertex ID
    public Vector<Long> adjacent = new Vector<Long>();  // the vertex neighbors

    Vertex()
    {

    }

    Vertex (short tag1, long group1,long VID1, Vector<Long> adjacent1)
    {
        this.tag=tag1;
        this.group=group1;
        this.VID=VID1;
        this.adjacent=adjacent1;
    }

    Vertex (short tag1,long group1)
    {
        this.tag=tag1;
        this.group=group1;
    }

    public String toString()
    {
        return "Vertex [tag" + tag + ", group=" + group + ", VID =" + VID + ", adjacent=" + adjacent + "]";
    }

    public void readFields(DataInput in) throws IOException
    {
        tag= in.readShort();
        group= in.readLong();
        VID= in.readLong();
    
        adjacent = new Vector<Long>();
        int n = in.readInt();
        for(long i=0;i<n;i++)
        {
         adjacent.add(in.readLong());
        }
    }
    public void write(DataOutput out) throws IOException
    {
        out.writeShort(tag);
        out.writeLong(group);
        out.writeLong(VID);
        int nsize = adjacent.size();
        out.writeInt(nsize);
        
        for(int i=0;i<nsize;i++)
        {
            out.writeLong(adjacent.get(i));
        }
    }

}

public class Graph 
{
    public static class Mapper1 extends Mapper<Object,Text,LongWritable,Vertex>
    {
    @Override
    public void map (Object Key,Text value,Context context)throws IOException, InterruptedException
    {
        Scanner sc = new Scanner(value.toString()).useDelimiter(",");
        long vid= sc.nextLong();
        Vertex vertex= new Vertex();
        vertex.tag=(short)0;
        vertex.VID=vid;
        vertex.group=vid;

        vertex.adjacent = new Vector<Long>();

        while(sc.hasNext()){
            vertex.adjacent.add(sc.nextLong());
        }
        context.write(new LongWritable(vertex.VID), vertex);
        sc.close();
        }
    }
    
    
    public static class Reducer1 extends Reducer<LongWritable,Vertex,LongWritable,Vertex>
    {
    public void reduce(LongWritable Key, Iterable<Vertex> values,Context context) throws IOException, InterruptedException
    {
            for(Vertex v : values)
            {
                Vertex vertex = new Vertex(v.tag,v.group,v.VID,v.adjacent);
                context.write(Key,vertex);
            }
        }
    }
    public static class Mapper2 extends Mapper<LongWritable,Vertex,LongWritable,Vertex>
    {
    public void map(LongWritable Key,Vertex values,Context context )throws IOException, InterruptedException
    {
            context.write(new LongWritable(values.VID),values);
            
            for(Long v : values.adjacent)
            {
                context.write(new LongWritable(v),new Vertex((short)1,values.group));  
            }
        }
    }
    public static class Reducer2 extends Reducer<LongWritable,Vertex,LongWritable,Vertex>
    {
    
    public void reduce(LongWritable vid,Iterable<Vertex> values,Context context)throws IOException, InterruptedException 
    {
            Vector<Long> adjacent2 = new Vector<Long>();
            long m=Long.MAX_VALUE;
            for(Vertex vertex : values)
            {
                if(vertex.tag == 0)
                {
                    adjacent2 = (Vector)vertex.adjacent.clone();
                }
                m = Math.min(m,vertex.group);
            }
            context.write(new LongWritable(m),new Vertex((short)0,m,vid.get(),adjacent2));
        }
    }

    public static class Mapper3 extends Mapper<LongWritable,Vertex,LongWritable,IntWritable>
    {
    public void map(LongWritable group, Vertex values,Context context) throws IOException, InterruptedException
        {
            context.write(group,new IntWritable(1));
        }
    }
    
    public static class Reducer3 extends Reducer<LongWritable, IntWritable,LongWritable,LongWritable>
    {
    public void reduce(LongWritable key, Iterable<IntWritable> values,Context context)throws IOException, InterruptedException
    {
            long sum = 0;
            for(IntWritable v: values)
            {
                sum+=v.get();   
            }
            context.write(key,new LongWritable(sum));
        }
    }

    public static void main ( String[] args ) throws Exception 
    {
        Job job = Job.getInstance();
        job.setJobName("My_Job_1");
        job.setJarByClass(Graph.class);
        
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Vertex.class);
        
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Vertex.class);
        
        job.setMapperClass(Mapper1.class);
        job.setReducerClass(Reducer1.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        SequenceFileOutputFormat.setOutputPath(job,new Path(args[1]+"/f0"));
        job.waitForCompletion(true);
        
        for ( short i = 0; i < 5; i++ ) {
            
            Configuration conf = new Configuration();
            Job job2 = Job.getInstance(conf,"My_Job_2."+i);
            job2.setJarByClass(Graph.class);
        
            job2.setOutputKeyClass(LongWritable.class);
            job2.setOutputValueClass(Vertex.class);
        
            job2.setMapOutputKeyClass(LongWritable.class);
            job2.setMapOutputValueClass(Vertex.class);
        
            job2.setMapperClass(Mapper2.class);
            job2.setReducerClass(Reducer2.class);
        
            job2.setInputFormatClass(SequenceFileInputFormat.class);
            job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        
            SequenceFileInputFormat.setInputPaths(job2,new Path(args[1]+"/f"+i));
            SequenceFileOutputFormat.setOutputPath(job2,new Path(args[1]+"/f"+(i+1)));
            job2.waitForCompletion(true);
        }
        
        Job job3 = Job.getInstance();
        job3.setJobName("My_Job_3");
        job3.setJarByClass(Graph.class);
        
        job3.setOutputKeyClass(LongWritable.class);
        job3.setOutputValueClass(LongWritable.class);
        
        job3.setMapOutputKeyClass(LongWritable.class);
        job3.setMapOutputValueClass(IntWritable.class);
        
        job3.setMapperClass(Mapper3.class);
        job3.setReducerClass(Reducer3.class);
        
        job3.setInputFormatClass(SequenceFileInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        
        SequenceFileInputFormat.setInputPaths(job3,new Path(args[1]+"/f5"));
        FileOutputFormat.setOutputPath(job3,new Path(args[2]));
        job3.waitForCompletion(true);
    }
}