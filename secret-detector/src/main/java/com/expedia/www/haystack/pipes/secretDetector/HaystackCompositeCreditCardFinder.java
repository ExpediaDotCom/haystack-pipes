/*
 * Copyright 2018 Expedia, Inc.
 *
 *       Licensed under the Apache License, Version 2.0 (the "License");
 *       you may not use this file except in compliance with the License.
 *       You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *       Unless required by applicable law or agreed to in writing, software
 *       distributed under the License is distributed on an "AS IS" BASIS,
 *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *       See the License for the specific language governing permissions and
 *       limitations under the License.
 *
 */
package com.expedia.www.haystack.pipes.secretDetector;

import io.dataapps.chlorine.finder.CompositeFinder;
import io.dataapps.chlorine.finder.Finder;
import io.dataapps.chlorine.pattern.CreditCardFinder;

import java.util.Collection;
import java.util.List;

public class HaystackCompositeCreditCardFinder implements Finder {

	private final CompositeFinder compositeFinder;

	HaystackCompositeCreditCardFinder(CompositeFinder compositeFinder) {
	    this.compositeFinder = compositeFinder;
    }
	/**
	 * Like the CompositeCreditCardFinder provided by chlorine-finder, but with a START_BLOCK that avoids the false
	 * positives that were occurring with the occasional GUID.
	 */
	@SuppressWarnings("unused") // used in finders_default.xml
    public HaystackCompositeCreditCardFinder() {
	    this(new CompositeFinder("Credit Card"));
		String START_BLOCK = "(\\b|^)";
		compositeFinder.add(new CreditCardFinder("Mastercard", START_BLOCK + "5[1-5][0-9]{2}(\\ |\\-|)[0-9]{4}(\\ |\\-|)[0-9]{4}(\\ |\\-|)[0-9]{4}(\\D|$)"));
		compositeFinder.add(new CreditCardFinder("Visa", START_BLOCK + "4[0-9]{3}(\\ |\\-|)[0-9]{4}(\\ |\\-|)[0-9]{4}(\\ |\\-|)[0-9]{4}(\\D|$)"));
		compositeFinder.add(new CreditCardFinder("AMEX", START_BLOCK + "(34|37)[0-9]{2}(\\ |\\-|)[0-9]{6}(\\ |\\-|)[0-9]{5}(\\D|$)"));
		compositeFinder.add(new CreditCardFinder("Diners Club 1", START_BLOCK + "30[0-5][0-9](\\ |\\-|)[0-9]{6}(\\ |\\-|)[0-9]{4}(\\D|$)"));
		compositeFinder.add(new CreditCardFinder("Diners Club 2", START_BLOCK + "(36|38)[0-9]{2}(\\ |\\-|)[0-9]{6}(\\ |\\-|)[0-9]{4}(\\D|$)"));
		compositeFinder.add(new CreditCardFinder("Discover", START_BLOCK + "6011(\\ |\\-|)[0-9]{4}(\\ |\\-|)[0-9]{4}(\\ |\\-|)[0-9]{4}(\\D|$)"));
		compositeFinder.add(new CreditCardFinder("JCB 1", START_BLOCK + "3[0-9]{3}(\\ |\\-|)[0-9]{4}(\\ |\\-|)[0-9]{4}(\\ |\\-|)[0-9]{4}(\\D|$)"));
		compositeFinder.add(new CreditCardFinder("JCB 2", START_BLOCK + "(2131|1800)[0-9]{11}(\\D|$)"));
	}

	@Override
	public List<String> find(Collection<String> samples) {
		return compositeFinder.find(samples);
	}

	@Override
	public List<String> find(String sample) {
		return compositeFinder.find(sample);
	}

	@Override
	public String getName() {
		return compositeFinder.getName();
	}

}
