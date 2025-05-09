package edu.harvard.hms.dbmi.avillach.hpds.data.storage;

import com.fasterxml.jackson.core.type.TypeReference;
import edu.harvard.hms.dbmi.avillach.hpds.data.genotype.VariableVariantMasks;
import edu.harvard.hms.dbmi.avillach.hpds.data.genotype.VariantMasks;
import edu.harvard.hms.dbmi.avillach.hpds.storage.FileBackedJsonIndexStorage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

public class FileBackedStorageVariantMasksImpl extends FileBackedJsonIndexStorage<Integer, ConcurrentHashMap<String, VariableVariantMasks>> implements Serializable {
    private static final long serialVersionUID = -1086729119489479152L;

    public FileBackedStorageVariantMasksImpl(File storageFile) throws FileNotFoundException {
        super(storageFile);
    }
    private static final TypeReference<ConcurrentHashMap<String, VariableVariantMasks>> typeRef
            = new TypeReference<ConcurrentHashMap<String, VariableVariantMasks>>() {};

    @Override
    public TypeReference<ConcurrentHashMap<String, VariableVariantMasks>> getTypeReference() {
        return typeRef;
    }
}
