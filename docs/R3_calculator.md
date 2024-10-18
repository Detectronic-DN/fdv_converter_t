
# r3_calculator Function
The function calculates the radius $ r_3 $ for an egg shape given:
- \( w \): width of the egg.
- \( h \): height of the egg.
- \( egg\_form \): an integer indicating the shape type (1 for typical egg shape, else for a different variant).

## Mathematical Steps Involved:

### 1. Initial Definitions:
- $ r_2 = \frac{w}{2} $: This is half of the width, representing the radius at the widest point.
- $ r_1 $: The radius at the narrower end of the egg. It is calculated based on the `egg_form` parameter:
   - If `egg_form == 1`:
     $$ r_1 = \frac{h - w}{2} $$
   - Else:
     $$ r_1 = \frac{h - w}{4} $$

- $ h_2 = h - r_2 $: This is the adjusted height based on the position of $ r_2 $.
- The initial guess for $ r_3 $ is set to $ r_3 = h $.

### 2. Iteration Process:
The function iteratively refines the value of $ r_3 $ until the difference between subsequent estimates is smaller than a precision threshold $ 10^{-5} $, or the iteration count exceeds 1000.

- **Offset Calculation**: The offset is calculated as:
  $$ \text{offset} = r_3 - r_2 $$

- **Square Term**: The square term is calculated as:
  $$ \text{square_term} = (r_3 - r_1)^2 - (h_2 - r_1)^2 $$

  If the square term is negative, a math domain error occurs, and the function returns \(-1.0\).

- **Offset Adjustment**: The square root of the square term is used to adjust the offset:
  $$ \text{offset_a} = \sqrt{\text{square_term}} $$

- **Difference Calculation**: The difference between the computed offset and the adjusted offset is:
  $$ \text{diff} = \text{offset} - \text{offset_a} $$

- **Update $ r_3 $**: The radius $ r_3 $ is updated using:
  $$ r_3 = r_3 + \frac{\text{diff}}{10} $$

- **Iteration Continues**: The loop continues until the absolute value of $ \text{diff} $ is smaller than $ 10^{-5} $, or the iteration limit is reached.

### 3. Error Handling:
- If the square term is negative, a math domain error occurs, and the function returns \(-1.0\).
- If the iterations exceed the maximum allowed without converging, the function also returns \(-1.0\).

### 4. Convergence:
Upon successful convergence, the final value of $ r_3 $ is printed and returned.
