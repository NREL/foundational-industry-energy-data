# Flowchart of FIED package


The syntax used here is:
- Root module (modules directly under fied) are upper case. Thus `fied.nei` is `NEI`.
- Each level of submodules is split by double underscore `__`. Thus `fied.nei.nei_EF_calculations` is `NEI__nei_EF_calculations`.
- Modules and functions repeat the the original name, even if that doesn't follow Python naming conventions. Thus `fied.nei.nei_EF_calculations.NEI.format_nei_char()` is `NEI__nei_EF_calculations__NEI__format_nei_char`. Note that the name in the diagram can't get (), but we use it in the label to make it clear it's a function.

Here is a full example:
`NEI__nei_EF_calculations__NEI__format_nei_char[label="nei.nei_EF_calculations.NEI.format_nei_char()"]`


- Group in sequence all the contents of a file and when convenient, use alphabetical order to make it easier for us to find things.

- The convention for the arrows is 'object -> used by'. Thus if function A calls function B, the arrow goes from B to A. For instance (check the flowchart to see this example):
  - ghgrp.get_GHGRP_data.get_GHGRP_records() is used to define the variable ghgrp.calc_GHGRP_energy.GHGRP.fac_file_2010.
  - The variable ghgrp.calc_GHGRP_energy.GHGRP.fac_file_2010 is used by ghgrp.calc_GHGRP_energy.GHGRP.fac_read_fix().
