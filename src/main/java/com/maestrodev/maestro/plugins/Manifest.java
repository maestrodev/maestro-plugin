/*
 * Copyright (c) 2013, MaestroDev. All rights reserved.
 */
package com.maestrodev.maestro.plugins;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;

/**
 * Helper class to handle Maestro plugin manifests, including basic manifest
 * validation.
 */
public class Manifest {
    private static final JsonParser parser = new JsonParser();

    private JsonArray manifest;
    private boolean validated = false;
    private List<String> errors = new ArrayList<String>();

    public Manifest(InputStream is) {
	Reader r = null;
	try {
	    r = new InputStreamReader(this.getClass().getClassLoader()
		    .getResourceAsStream("manifest.json"));
	    manifest = parser.parse(r).getAsJsonArray();
	} catch (JsonParseException e) {
	    validated = true;
	    addError("Error parsing manifest: " + e.getMessage());
	} finally {
	    IOUtils.closeQuietly(r);
	}
    }

    public List<String> getErrors() {
	return errors;
    }

    public JsonArray getPlugins() {
	return manifest;
    }

    public boolean isValid() {
	validate();
	return errors.isEmpty();
    }

    private void addError(String error) {
	errors.add(error);
    }

    private void validate(boolean condition, String error) {
	if (!condition)
	    addError(error);
    }

    public void validate() {
	if (!validated) {
	    JsonArray plugins = manifest.getAsJsonArray();
	    for (JsonElement plugin : plugins) {
		validatePlugin(plugin);
	    }
	}
	validated = true;
    }

    private void validatePlugin(JsonElement plugin) {
	validate(plugin.isJsonObject(), "plugin is not an object");
	if (plugin.isJsonObject()) {
	    JsonObject p = plugin.getAsJsonObject();
	    String[] fields = { "name", "description", "author", "version",
		    "type", "class", "dependencies" };
	    for (String f : fields) {
		validate(p.has(f), "plugin doesn't have field " + f);
	    }
	}
    }
}
