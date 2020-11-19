/*
 * Hibernate Utilities
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the LICENSE file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package com.tickerfit.domain.utility;

public interface EntityGroupingIdentifier {

    /*
    EXAMPLE Implementation:
    return someInstanceSpecificGroupingValue; // e.g. return (gender=="MALE")?"MAN":"WOMAN"

    Entities would now have ids generated like MAN-00001,MAN-00002... and WOMAN-00001,WOMAN-00002

    ** Note: if multiple different entity classes return the same EntityReferenceGroupPrefix(s)
    they will also share the same id generation index and so id's generated for any given
    entity class may not be contiguous in this case.

     */
    String getEntityReferenceGroupPrefix();
}
