/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.migration;

import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.migration.data.MigrationIdentifier;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.stream.LongStream;

import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class MigrationManagerTest {

    @Test
    public void partition() {
        long[] partitions = MigrationManager.partition(50, 100, 16);

        assertEquals(16 * 2, partitions.length);

        int chunkCount = 0;

        for (int i = 0; i < partitions.length; i += 2) {
            chunkCount += partitions[i + 1] - partitions[i];
        }

        assertEquals(50, chunkCount);

        // Verify that each range's end equals the next range's start
        for (int i = 1; i < partitions.length - 2; i += 2) {
            assertEquals(partitions[i], partitions[i + 1]);
        }
    }
}