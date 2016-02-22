package de.hhu.bsinfo.dxcompute.run;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import de.hhu.bsinfo.dxcompute.DXCompute;
import de.hhu.bsinfo.dxcompute.NullPipeline;
import de.hhu.bsinfo.dxcompute.Pipeline;
import de.hhu.bsinfo.dxram.run.DXRAMMain;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.utils.Pair;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.main.Main;

/**
 * Main entry point to start a computing pipeline with DXCompute.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 18.02.16
 *
 */
public class DXComputePipeline extends DXRAMMain {

	public static final Pair<String, String> ARG_PIPELINE = new Pair<String, String>("pipeline", NullPipeline.class.getName());
	
	/**
	 * Main entry point.
	 * @param args Console arguments.
	 */
	public static void main(final String[] args) {
		Main main = new DXComputePipeline();
		main.run(args);
	}
	
	/**
	 * Constructor
	 */
	protected DXComputePipeline() 
	{
		super(null, null, NodeRole.PEER);
	}
	
	@Override
	protected void registerDefaultProgramArguments(ArgumentList p_arguments) {
		super.registerDefaultProgramArguments(p_arguments);
		p_arguments.setArgument(ARG_PIPELINE);
	}

	@Override
	protected int mainApplication(ArgumentList p_arguments) {
		System.out.println("DXCompute Peer started");

		// create pipeline using reflection
		String pipelineName = p_arguments.getArgument(ARG_PIPELINE);
		System.out.println("Executing pipeline: " + pipelineName);
		
		Pipeline pipeline = getPipeline(pipelineName, p_arguments);
		if (pipeline == null) {
			return -1;
		}
		
		DXCompute dxgraph = new DXCompute(getDXRAM());
		if (!dxgraph.executePipeline(pipeline)) {
			return -2;
		}
		
		return 0;
	}
	
	/**
	 * Get a pipeline by its class name.
	 * @param p_name Full class name (with package path). 
	 * @param p_arguments Argument list for the pipeline.
	 * @return If found, returns an instance to the pipeline, null otherwise.
	 */
	private Pipeline getPipeline(final String p_name, final ArgumentList p_arguments) {
		Class<?> clazz = null;
		try {
			clazz = Class.forName(p_name);
		} catch (ClassNotFoundException e) {
			System.out.println("Could not find class " + p_name + " to create pipeline instance.");
			return null;
		}
		
		Constructor<?> ctor = null;
		
		try {
			ctor = clazz.getConstructor(ArgumentList.class);
		} catch (NoSuchMethodException | SecurityException e1) {
			System.out.println("Could not get default constructor of pipeline " + p_name + ".");
			return null;
		}
		
		Pipeline pipeline = null;
		try {
			pipeline = (Pipeline) ctor.newInstance(p_arguments);
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			System.out.println("Could not create instance of pipeline " + p_name + ".");
		}
		
		return pipeline;
	}
}