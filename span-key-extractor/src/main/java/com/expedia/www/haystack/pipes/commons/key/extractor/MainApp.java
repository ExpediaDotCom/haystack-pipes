package com.expedia.www.haystack.pipes.commons.key.extractor;

public class MainApp {

    public static void main(String[] args) {
        ProjectConfiguration projectConfiguration = new ProjectConfiguration();
        System.out.println(projectConfiguration.getDirectory());
        for (String key: projectConfiguration.getSpanExtractorConfigs().keySet()){
            System.out.println(projectConfiguration.getSpanExtractorConfigs().get(key));
        }
    }
}
