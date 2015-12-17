package de.uniduesseldorf.dxram.core.data;

import de.uniduesseldorf.utils.serialization.Exportable;
import de.uniduesseldorf.utils.serialization.Importable;

/**
 * Interface for any kind of data structure that can be stored and read from 
 * memory. Implement this with any object you want to put/get from the memory system.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 11.12.15
 */
public interface DataStructure extends Importable, Exportable
{
	/**
	 * Get the unique identifier of this data structure.
	 * @return Unique identifier.
	 */
	public long getID();
}
